{-# LANGUAGE TypeInType #-}
{-# LANGUAGE UnboxedTuples #-}
module Data.HashTable.RobinHood
  ( SafeHash(..)
  , safeHash
  , emptyHash
  , tombstoneHash

  , Bucket(..)

  , HashTable(..)
  , IOHashTable

  , new
  , insert
  , lookup
  , delete
  ) where

import           Prelude hiding (init, lookup)

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

-- | SafeHash needs to be able to store two special values
-- representing empty buckets and tombstones. We accomplish this by
-- setting the 2 highest bits to 1 for all other values.
newtype SafeHash = SafeHash Int deriving (Eq, Prim)

{-# INLINABLE emptyHash #-}
emptyHash :: SafeHash
emptyHash = SafeHash (1 `shiftL` (finiteBitSize (undefined :: Int) - 1))

{-# INLINABLE tombstoneHash #-}
tombstoneHash :: SafeHash
tombstoneHash = SafeHash (1 `shiftL` (finiteBitSize (undefined :: Int) - 2))

{-# INLINABLE safeHash #-}
safeHash :: Hashable a => a -> SafeHash
safeHash a =
  let !h = Hashable.hash a
   in SafeHash ((3 `shiftL` (finiteBitSize (undefined :: Int) - 2)) .|. h)

data Bucket k v = Bucket !k v

data HashTable s k v = HashTable
  { hashes :: {-# UNPACK #-}!(MutVar s (MutablePrimArray s SafeHash))
  , buckets :: {-# UNPACK #-}!(MutVar s (MutableArray s (Bucket k v)))
  , numItems :: {-# UNPACK #-}!(PrimRef s Int)
  } deriving Eq

type IOHashTable k v = HashTable (PrimState IO) k v

-- @new capacity@ creates a new `HashTable` of the given capacity. The
-- capacity must be 0 or a power of two.
{-# INLINABLE new #-}
new :: PrimMonad m => Int -> m (HashTable (PrimState m) k v)
new initSize = do
  hs <- newMutVar undefined
  bs <- newMutVar undefined
  items <- newPrimRef 0
  let !table = HashTable hs bs items
  init table initSize
  pure table

{-# INLINABLE isPowerOfTwo #-}
isPowerOfTwo :: Int -> Bool
isPowerOfTwo i = i .&. (i - 1) == 0

{-# INLINABLE init #-}
init :: PrimMonad m => HashTable (PrimState m) k v -> Int -> m ()
init table initSize = assert (isPowerOfTwo initSize) $ do
  hs <- newPrimArray initSize
  setPrimArray hs 0 initSize emptyHash
  writeMutVar (hashes table) hs
  -- We will never access the values if the hash is set to `emptyHash` so this is safe.
  bs <- newArray initSize undefined
  writeMutVar (buckets table) bs
  writePrimRef (numItems table) 0

data VacantType
  = NoElem
  | NeqElem {-# UNPACK #-}!Int

data BucketType = Occupied | Vacant !VacantType

bucketFor :: (Eq k, PrimMonad m) => HashTable (PrimState m) k v -> SafeHash -> k -> m (Int, BucketType)
bucketFor table h k = do
  hs <- readMutVar (hashes table)
  bs <- readMutVar (buckets table)
  let !numBuckets = sizeofMutablePrimArray hs
      go !i !currentDisplacement = do
        h' <- readPrimArray hs i
        if h' == emptyHash
          then pure (i, Vacant NoElem)
          else do
            let !storedDisplacement = displacement numBuckets h' i
            if storedDisplacement < currentDisplacement
              then pure (i, Vacant (NeqElem storedDisplacement))
              else if h' == h
                     then do
                       Bucket k' _ <- readArray bs i
                       if k' == k
                         then pure (i, Occupied)
                         else go
                                (nextBucket numBuckets i)
                                (currentDisplacement + 1)
                     else go (nextBucket numBuckets i) (currentDisplacement + 1)
  go (bucketIndex numBuckets h) 0

{-# INLINABLE bucketIndex #-}
bucketIndex :: Int -> SafeHash -> Int
bucketIndex numBuckets (SafeHash h) = h .&. (numBuckets - 1)

-- | Insert a new key/value pair in the hash table.
insert :: (Eq k, Hashable k, PrimMonad m) => HashTable (PrimState m) k v -> k -> v -> m ()
insert table k v = do
  reserve table 1
  (!i, !type') <- bucketFor table h k
  hs <- readMutVar (hashes table)
  bs <- readMutVar (buckets table)
  case type' of
    Occupied -> writeArray bs i (Bucket k v)
    Vacant el -> do
      n <- readPrimRef (numItems table)
      writePrimRef (numItems table) (n + 1)
      case el of
        NoElem -> do
          writePrimArray hs i h
          writeArray bs i (Bucket k v)
        NeqElem disp -> robinHood hs bs h k v i disp
  where
    !h = safeHash k

robinHood :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> SafeHash -> k -> v -> Int -> Int -> m ()
robinHood hs bs hash key value pos disp = go hash (Bucket key value) pos disp
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

getEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> m (SafeHash, Bucket k v)
getEntry hs bs i = do
  h <- readPrimArray hs i
  b <- readArray bs i
  pure (h, b)

setEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> SafeHash -> Bucket k v -> m ()
setEntry hs bs i h b = do
  writePrimArray hs i h
  writeArray bs i b

deleteEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> m ()
deleteEntry hs bs i = do
  writePrimArray hs i emptyHash
  writeArray bs i undefined

swapEntry :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> Int -> SafeHash -> Bucket k v -> m (SafeHash, Bucket k v)
swapEntry hs bs i h b = do
  (h', b') <- getEntry hs bs i
  setEntry hs bs i h b
  pure (h', b')

reserve :: PrimMonad m => HashTable (PrimState m) k v -> Int -> m ()
reserve table additionalElems = do
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

-- | Lookup the value at a key in the hash table.
lookup :: (Eq k, Hashable k, PrimMonad m) => HashTable (PrimState m) k v -> k -> m (Maybe v)
lookup table k = do
  mayI <- findKey k table
  case mayI of
    Nothing -> pure Nothing
    Just i -> do
      bs <- readMutVar (buckets table)
      Bucket _ v <- readArray bs i
      pure (Just v)

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

displacement :: Int -> SafeHash -> Int -> Int
displacement numBuckets hash pos
  | idealPos <= pos = pos - idealPos
  | otherwise = pos + (numBuckets - idealPos)
  where !idealPos = bucketIndex numBuckets hash

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
orderedInsert :: PrimMonad m => MutablePrimArray (PrimState m) SafeHash -> MutableArray (PrimState m) (Bucket k v) -> SafeHash -> Bucket k v -> m ()
orderedInsert hs bs h b = go (bucketIndex numBuckets h)
  where
    !numBuckets = sizeofMutablePrimArray hs
    go !i = do
      h' <- readPrimArray hs i
      if h' == emptyHash
        then setEntry hs bs i h b
        else go (nextBucket numBuckets i)
