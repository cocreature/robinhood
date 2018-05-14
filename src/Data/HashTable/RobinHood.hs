{-# LANGUAGE MagicHash #-}
{-# LANGUAGE TypeInType #-}
{-# LANGUAGE UnboxedSums #-}
{-# LANGUAGE UnboxedTuples #-}
module Data.HashTable.RobinHood
  ( SafeHash(..)
  , safeHash
  , emptyHash

  , Bucket(..)

  , HashTable(..)
  , IOHashTable

  , new
  , insert
  , lookup
  , delete
  ) where

import           Prelude hiding (lookup)

import           Control.Exception
import           Control.Monad
import           Control.Monad.Primitive
import           Data.Bits
import           Data.Hashable (Hashable)
import qualified Data.Hashable as Hashable
import           Data.Primitive.Array
import           Data.Primitive.MutVar
import           Data.Primitive.PrimArray
import           Data.Primitive.PrimRef
import           Data.Primitive.Types
import           GHC.Exts

-- | SafeHash needs to be able to store a special value representing
-- empty buckets. We accomplish this by setting the 1 highest bits to
-- 1 for all other values leaving 0 for `emptyHash`.
newtype SafeHash = SafeHash Int deriving (Eq, Prim)

{-# INLINABLE emptyHash #-}
emptyHash :: SafeHash
emptyHash = SafeHash 0

{-# INLINABLE safeHash #-}
safeHash :: Hashable a => a -> SafeHash
safeHash a =
  let !h = Hashable.hash a
   in SafeHash ((1 `shiftL` (finiteBitSize (undefined :: Int) - 1)) .|. h)

data Bucket k v = Bucket !k v

data HashTable s k v = HashTable
  { hashes :: {-# UNPACK #-}!(MutVar s (MutablePrimArray s SafeHash))
  , buckets :: {-# UNPACK #-}!(MutVar s (MutableArray s (Bucket k v)))
  , numItems :: {-# UNPACK #-}!(PrimRef s Int)
  } deriving Eq

type IOHashTable k v = HashTable (PrimState IO) k v

----------------
-- Public API --
----------------

-- @new capacity@ creates a new `HashTable` of the given capacity. The
-- capacity must be 0 or a power of two.
{-# INLINABLE new #-}
new :: PrimMonad m => Int -> m (HashTable (PrimState m) k v)
new initSize = assert (isPowerOfTwo initSize) $ do
  hs <- newPrimArray initSize
  setPrimArray hs 0 initSize emptyHash
  bs <- newArray initSize undefined
  HashTable
    <$> newMutVar hs
    <*> newMutVar bs
    <*> newPrimRef 0

-- | Insert a new key/value pair in the hash table.
{-# INLINABLE insert #-}
insert :: (Eq k, Hashable k, PrimMonad m) => HashTable (PrimState m) k v -> k -> v -> m ()
insert table k v = do
  reserve table 1
  hs <- readMutVar (hashes table)
  bs <- readMutVar (buckets table)
  BucketFor i type' <- bucketFor hs bs h k
  case type' of
    (# (# #) | | #) -> writeArray bs i (Bucket k v)
    (# | (# #) | #) -> do
      n <- readPrimRef (numItems table)
      writePrimRef (numItems table) (n + 1)
      writePrimArray hs i h
      writeArray bs i (Bucket k v)
    (# | | disp #) -> do
      n <- readPrimRef (numItems table)
      writePrimRef (numItems table) (n + 1)
      robinHood hs bs h k v i (I# disp)
  where
    !h = safeHash k

-- | Lookup the value at a key in the hash table.
{-# INLINABLE lookup #-}
lookup :: (Eq k, Hashable k, PrimMonad m) => HashTable (PrimState m) k v -> k -> m (Maybe v)
lookup table k = do
  mayI <- findKey k table
  case mayI of
    Nothing -> pure Nothing
    Just i -> do
      bs <- readMutVar (buckets table)
      Bucket _ v <- readArray bs i
      pure (Just v)

-- | Delete the value at a key and return it.
--
-- If the key was not present in the hash table, `Nothing` is returned.
{-# INLINABLE delete #-}
delete :: (Eq k, Hashable k, PrimMonad m) => HashTable (PrimState m) k v -> k -> m (Maybe v)
delete table k = do
  mayI <- findKey k table
  case mayI of
    Nothing -> pure Nothing
    Just pos -> do
      numItems' <- readPrimRef (numItems table)
      writePrimRef (numItems table) (numItems' - 1)
      hs <- readMutVar (hashes table)
      bs <- readMutVar (buckets table)
      Bucket _ v <- readArray bs pos
      let !numBuckets = sizeofMutablePrimArray hs
          go i = do
            let !i' = nextBucket numBuckets i
            h <- readPrimArray hs i'
            if | h == emptyHash || displacement numBuckets h i' == 0 ->
                 deleteEntry hs bs i
               | otherwise -> do
                   b <- readArray bs i'
                   setEntry hs bs i h b
                   go i'
      go pos
      pure (Just v)

----------------------
-- Internal helpers --
----------------------

{-# INLINABLE isPowerOfTwo #-}
isPowerOfTwo :: Int -> Bool
isPowerOfTwo i = i .&. (i - 1) == 0

-- | The Int represents the index of the bucket. The unboxed sum represents (in this order):
--
-- 1. A field with a matching key was found.
-- 2. An empty bucket was found.
-- 3. We found an element with a smaller displacement. The Int#
-- represents that displacement.
data BucketFor = BucketFor {-# UNPACK #-} !Int (# (# #) | (# #) | Int# #)

{-# INLINABLE bucketFor #-}
bucketFor :: (Eq k, PrimMonad m) => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> SafeHash -> k -> m BucketFor
bucketFor hs bs h k = do
  let !numBuckets = sizeofMutablePrimArray hs
      go !i !currentDisplacement = do
        h' <- readPrimArray hs i
        if h' == emptyHash
          then pure (BucketFor i (# | (# #) | #))
          else do
            let !storedDisplacement@(I# disp) = displacement numBuckets h' i
            if storedDisplacement < currentDisplacement
              then pure (BucketFor i (# | | disp #))
              else if h' == h
                     then do
                       Bucket k' _ <- readArray bs i
                       if k' == k
                         then pure (BucketFor i (# (# #) | | #))
                         else go (nextBucket numBuckets i) (currentDisplacement + 1)
                     else go (nextBucket numBuckets i) (currentDisplacement + 1)
  go (bucketIndex numBuckets h) 0

{-# INLINABLE bucketIndex #-}
bucketIndex :: Int -> SafeHash -> Int
bucketIndex numBuckets (SafeHash h) = h .&. (numBuckets - 1)

{-# INLINABLE robinHood #-}
robinHood :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> SafeHash -> k -> v -> Int -> Int -> m ()
robinHood hs bs !hash key value pos disp = go hash (Bucket key value) pos disp
  where
    go h b !i !d = do
      (h', b') <- swapEntry hs bs i h b
      let go' !j !currentDisplacement = do
            h'' <- readPrimArray hs j
            if h'' == emptyHash
              then setEntry hs bs j h' b'
              else do
                let !storedDisplacement = displacement numBuckets h'' j
                if storedDisplacement < currentDisplacement
                  then go h' b' j storedDisplacement
                  else go' (nextBucket numBuckets j) (currentDisplacement + 1)
      go' (nextBucket numBuckets i) (d + 1)
    !numBuckets = sizeofMutablePrimArray hs

-- | Load factor of 0.9
capacity :: Int -> Int
capacity c = (c * 10 + 10 - 1) `div` 11

capacityFor :: Int -> Int
capacityFor 0 = 0
capacityFor c = max 16 (nextPowerOfTwo (c * 11 `div` 10))

nextPowerOfTwo :: Int -> Int
nextPowerOfTwo i = 2 ^ (logBase2 (i - 1) + 1)

logBase2 :: Int -> Int
logBase2 x = finiteBitSize x - 1 - countLeadingZeros x

{-# INLINABLE getEntry #-}
getEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> m (SafeHash, Bucket k v)
getEntry hs bs i = do
  h <- readPrimArray hs i
  b <- readArray bs i
  pure (h, b)

{-# INLINABLE setEntry #-}
setEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> SafeHash -> Bucket k v -> m ()
setEntry hs bs i h b = do
  writePrimArray hs i h
  writeArray bs i b

{-# INLINABLE deleteEntry #-}
deleteEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> m ()
deleteEntry hs bs i = do
  writePrimArray hs i emptyHash
  writeArray bs i undefined

{-# INLINABLE swapEntry #-}
swapEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> SafeHash -> Bucket k v -> m (SafeHash, Bucket k v)
swapEntry hs bs i h b = do
  (h', b') <- getEntry hs bs i
  setEntry hs bs i h b
  pure (h', b')

{-# INLINABLE reserve #-}
reserve :: PrimMonad m => HashTable (PrimState m) k v -> Int -> m ()
reserve table !additionalElems = do
  hs <- readMutVar (hashes table)
  !currentNumItems <- readPrimRef (numItems table)
  let !numBuckets = sizeofMutablePrimArray hs
      !newNumItems = currentNumItems + additionalElems
      !remaining = capacity numBuckets - currentNumItems
  when (remaining < additionalElems) $ do
    let !newNumBuckets = capacityFor newNumItems
    bs <- readMutVar (buckets table)
    hs' <- newPrimArray newNumBuckets
    setPrimArray hs' 0 newNumBuckets emptyHash
    bs' <- newArray newNumBuckets undefined
    writeMutVar (hashes table) hs'
    writeMutVar (buckets table) bs'
    when (currentNumItems /= 0) $ do
      let go !i !toInsert = do
            h <- readPrimArray hs i
            if h == emptyHash
              then go (nextBucket numBuckets i) toInsert
              else do
                b <- readArray bs i
                orderedInsert hs' bs' h b
                when
                  (toInsert > 1)
                  (go (nextBucket numBuckets i) (toInsert - 1))
      start <- firstIdeal hs
      go start currentNumItems

displacement :: Int -> SafeHash -> Int -> Int
displacement numBuckets hash pos
  | idealPos <= pos = pos - idealPos
  | otherwise = pos + (numBuckets - idealPos)
  where !idealPos = bucketIndex numBuckets hash

{-# INLINABLE findKey #-}
findKey :: (Eq k, Hashable k, PrimMonad m) => k -> HashTable (PrimState m) k v -> m (Maybe Int)
findKey k table = do
  hs <- readMutVar (hashes table)
  bs <- readMutVar (buckets table)
  let !numBuckets = sizeofMutablePrimArray hs
  if numBuckets == 0
    then pure Nothing
    else
      let go !currentDisplacement !i = do
            storedHash <- readPrimArray hs i
            let !storedDisplacement = displacement numBuckets storedHash i
            if | storedHash == emptyHash -> pure Nothing
               | currentDisplacement > storedDisplacement -> pure Nothing
               | currentHash == storedHash -> do
                   Bucket k' _ <- readArray bs i
                   if k == k'
                     then pure (Just i)
                     else go (currentDisplacement + 1) (nextBucket numBuckets i)
               | otherwise -> go (currentDisplacement + 1) (nextBucket numBuckets i)
      in go 0 (bucketIndex numBuckets currentHash)
  where
    !currentHash = safeHash k

nextBucket :: Int -> Int -> Int
nextBucket numBuckets i = (i + 1) .&. (numBuckets - 1)

-- | Assumes that the size of the array is not 0 and that it contains
-- at least one element with ideal placement.
{-# INLINABLE firstIdeal #-}
firstIdeal :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> m Int
firstIdeal hs = go 0
  where
    !numBuckets = sizeofMutablePrimArray hs
    go !i = do
      h <- readPrimArray hs i
      if | h == emptyHash -> go (nextBucket numBuckets i)
         | displacement numBuckets h i == 0 -> pure i
         | otherwise -> go (nextBucket numBuckets i)


-- | Insertion function used during reinsertion.
{-# INLINABLE orderedInsert #-}
orderedInsert :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> SafeHash -> Bucket k v -> m ()
orderedInsert hs bs h b = go (bucketIndex numBuckets h)
  where
    !numBuckets = sizeofMutablePrimArray hs
    go !i = do
      h' <- readPrimArray hs i
      if h' == emptyHash
        then setEntry hs bs i h b
        else go (nextBucket numBuckets i)
