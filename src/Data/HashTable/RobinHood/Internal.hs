{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE TypeInType #-}
{-# LANGUAGE UnboxedTuples #-}
module Data.HashTable.RobinHood.Internal
  ( SafeHash(..)
  , safeHash
  , emptyHash

  , Bucket(..)

  , HashTable(..)

  , new
  , insert
  , lookup
  , delete
  , fromList
  , mapM_
  , size
  , capacity

  -- * Internal utilities
  , nextPowerOfTwo
  , logBase2
  ) where

import           Prelude hiding (lookup, mapM_)

import           Control.Monad hiding (mapM_)
import           Control.Monad.Primitive
import           Data.Bits
import           Data.Foldable (for_)
import           Data.Hashable (Hashable)
import qualified Data.Hashable as Hashable
import           Data.Primitive.Contiguous (Contiguous, Element, Mutable)
import qualified Data.Primitive.Contiguous as Contiguous
import           Data.Primitive.MutVar
import           Data.Primitive.PrimArray
import           Data.Primitive.PrimRef
import           Data.Primitive.Types

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

data HashTable ak av s k v = HashTable
  { hashes :: {-# UNPACK #-}!(MutVar s (MutablePrimArray s SafeHash))
  , keys :: {-# UNPACK #-}!(MutVar s (Mutable ak s k))
  , values :: {-# UNPACK #-}!(MutVar s (Mutable av s v))
  , numItems :: {-# UNPACK #-}!(PrimRef s Int)
  } deriving (Eq)

----------------
-- Public API --
----------------

-- @new capacity@ creates a new `HashTable` of the given capacity.
{-# INLINABLE new #-}
new :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => Int -> m (HashTable ak av (PrimState m) k v)
new requestedSize = do
  let initSize =
        if requestedSize < 0
          then 0
          else nextPowerOfTwo requestedSize
  hs <- newPrimArray initSize
  setPrimArray hs 0 initSize emptyHash
  ks <- stToPrim (Contiguous.new initSize)
  vs <- stToPrim (Contiguous.new initSize)
  HashTable <$> newMutVar hs <*> newMutVar ks <*> newMutVar vs <*> newPrimRef 0

-- | Insert a new key/value pair in the hash table.
{-# INLINABLE insert #-}
insert :: (Eq k, Hashable k, PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => HashTable ak av (PrimState m) k v -> k -> v -> m ()
insert table k v =
  withReserved continue table 1
  where
    continue !hs !ks !vs = do
      let matchingKey i =
            stToPrim (Contiguous.write ks i k >> Contiguous.write vs i v)
          emptyBucket i = do
            n <- readPrimRef (numItems table)
            writePrimRef (numItems table) (n + 1)
            writePrimArray hs i h
            stToPrim (Contiguous.write ks i k)
            stToPrim (Contiguous.write vs i v)
          smallerDisplacement i disp = do
            n <- readPrimRef (numItems table)
            writePrimRef (numItems table) (n + 1)
            robinHood hs ks vs h k v i disp
      withBucketFor matchingKey emptyBucket smallerDisplacement hs ks h k
    !h = safeHash k

-- | Lookup the value at a key in the hash table.
{-# INLINABLE lookup #-}
lookup :: (Eq k, Hashable k, PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => HashTable ak av (PrimState m) k v -> k -> m (Maybe v)
lookup table k = do
  mayI <- findKey k table
  case mayI of
    Nothing -> pure Nothing
    Just i -> do
      vs <- readMutVar (values table)
      v <- stToPrim (Contiguous.read vs i)
      pure (Just v)

-- | Delete the value at a key and return it.
--
-- If the key was not present in the hash table, `Nothing` is returned.
{-# INLINABLE delete #-}
delete :: (Eq k, Hashable k, PrimMonad m, Contiguous av, Element av v, Contiguous ak, Element ak k) => HashTable ak av (PrimState m) k v -> k -> m (Maybe v)
delete table k = do
  mayI <- findKey k table
  case mayI of
    Nothing -> pure Nothing
    Just pos -> do
      numItems' <- readPrimRef (numItems table)
      writePrimRef (numItems table) (numItems' - 1)
      hs <- readMutVar (hashes table)
      ks <- readMutVar (keys table)
      vs <- readMutVar (values table)
      v <- stToPrim (Contiguous.read vs pos)
      let !numBuckets = sizeofMutablePrimArray hs
          go i = do
            let !i' = nextBucket numBuckets i
            h <- readPrimArray hs i'
            if | h == emptyHash || displacement numBuckets h i' == 0 ->
                 deleteEntry hs ks vs i
               | otherwise -> do
                   k' <- stToPrim (Contiguous.read ks i')
                   v' <- stToPrim (Contiguous.read vs i')
                   setEntry hs ks vs i h k' v'
                   go i'
      go pos
      pure (Just v)

-- | Construct a `HashTable` from a list of key-value pairs. For
-- duplicate keys, the last value is the one stored in the final
-- hashtable.
{-# INLINABLE fromList #-}
fromList :: (Eq k, Hashable k, PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => [(k, v)] -> m (HashTable ak av (PrimState m) k v)
fromList xs = do
  table <- new 0
  for_ xs $ \(k,v) -> insert table k v
  pure table

-- | Monadic map over the entries in the `HashTable`. No guarantees
-- are made about the order in which the entries are traversed.
{-# INLINABLE mapM_ #-}
mapM_ :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => (k -> v -> m a) -> HashTable ak av (PrimState m) k v -> m ()
mapM_ f table = do
  hs <- readMutVar (hashes table)
  ks <- readMutVar (keys table)
  vs <- readMutVar (values table)
  for_ [0 .. sizeofMutablePrimArray hs - 1] $ \i -> do
    h <- readPrimArray hs i
    when (h /= emptyHash) $ do
      k <- stToPrim (Contiguous.read ks i)
      v <- stToPrim (Contiguous.read vs i)
      _ <- f k v
      pure ()

-- | Returns the number of entries that are currently in the
-- `HashTable`.
{-# INLINABLE size #-}
size :: PrimMonad m => HashTable ak av (PrimState m) k v -> m Int
size table = readPrimRef (numItems table)

-- | Returns the current capacity, i.e., the number of allocated
-- buckets. This is guaranteed to be greater or equal to `size`.
{-# INLINABLE capacity #-}
capacity :: PrimMonad m => HashTable ak av (PrimState m) k v -> m Int
capacity table = sizeofMutablePrimArray <$> readMutVar (hashes table)

----------------------
-- Internal helpers --
----------------------

{-# INLINABLE withBucketFor #-}
withBucketFor ::
     (Eq k, PrimMonad m, Contiguous ak, Element ak k)
  => (Int -> m a)
  -> (Int -> m a)
  -> (Int -> Int -> m a)
  -> MutablePrimArray (PrimState m) SafeHash
  -> Mutable ak (PrimState m) k
  -> SafeHash
  -> k
  -> m a
withBucketFor matchingKey emptyBucket smallerDisplacement hs !ks h !k = do
  let !numBuckets = sizeofMutablePrimArray hs
      go !i !currentDisplacement = do
        h' <- readPrimArray hs i
        if h' == emptyHash
          then emptyBucket i
          else do
            let !storedDisplacement = displacement numBuckets h' i
            if | storedDisplacement < currentDisplacement -> smallerDisplacement i storedDisplacement
               | h' == h -> do
                   k' <- stToPrim (Contiguous.read ks i)
                   if k' == k
                     then matchingKey i
                     else go (nextBucket numBuckets i) (currentDisplacement + 1)
               | otherwise ->
                   go (nextBucket numBuckets i) (currentDisplacement + 1)
  go (bucketIndex numBuckets h) 0

{-# INLINABLE bucketIndex #-}
bucketIndex :: Int -> SafeHash -> Int
bucketIndex numBuckets (SafeHash h) = h .&. (numBuckets - 1)

{-# INLINE robinHood #-}
robinHood :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => MutablePrimArray (PrimState m) SafeHash -> Mutable ak (PrimState m) k -> Mutable av (PrimState m) v -> SafeHash -> k -> v -> Int -> Int -> m ()
robinHood !hs !ks !vs !hash !key value pos disp = go hash key value pos disp
  where
    go h k v !i !d = do
      (h', k', v') <- swapEntry hs ks vs i h k v
      let go' !j !currentDisplacement = do
            h'' <- readPrimArray hs j
            if h'' == emptyHash
              then setEntry hs ks vs j h' k' v'
              else do
                let !storedDisplacement = displacement numBuckets h'' j
                if storedDisplacement < currentDisplacement
                  then go h' k' v' j storedDisplacement
                  else go' (nextBucket numBuckets j) (currentDisplacement + 1)
      go' (nextBucket numBuckets i) (d + 1)
    !numBuckets = sizeofMutablePrimArray hs

-- | Load factor of 0.9
capacity' :: Int -> Int
capacity' c = (c * 10 + 10 - 1) `div` 11

capacityFor :: Int -> Int
capacityFor 0 = 0
capacityFor c = max 16 (nextPowerOfTwo (c * 11 `div` 10))

nextPowerOfTwo :: Int -> Int
nextPowerOfTwo i = 2 ^ (logBase2 (i - 1) + 1)

logBase2 :: Int -> Int
logBase2 x = finiteBitSize x - 1 - countLeadingZeros x

{-# INLINABLE getEntry #-}
getEntry :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => MutablePrimArray (PrimState m) SafeHash -> Mutable ak (PrimState m) k -> Mutable  av (PrimState m) v -> Int -> m (SafeHash, k, v)
getEntry hs ks vs i = do
  h <- readPrimArray hs i
  k <- stToPrim (Contiguous.read ks i)
  v <- stToPrim (Contiguous.read vs i)
  pure (h, k, v)

{-# INLINABLE setEntry #-}
setEntry :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => MutablePrimArray (PrimState m) SafeHash -> Mutable ak (PrimState m) k -> Mutable av (PrimState m) v -> Int -> SafeHash -> k -> v -> m ()
setEntry hs ks vs i h k v = do
  writePrimArray hs i h
  stToPrim (Contiguous.write ks i k)
  stToPrim (Contiguous.write vs i v)

{-# INLINABLE deleteEntry #-}
deleteEntry :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => MutablePrimArray (PrimState m) SafeHash -> Mutable ak (PrimState m) k -> Mutable av (PrimState m) v -> Int -> m ()
deleteEntry hs _ks _vs i = do
  writePrimArray hs i emptyHash
  -- TODO For a boxed array, we want to set the ks and vs array to undefined
  -- stToPrim (Contiguous.write ks i (error "undefined key"))
  -- stToPrim (Contiguous.write vs i (error "undefined value"))

{-# INLINABLE swapEntry #-}
swapEntry :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => MutablePrimArray (PrimState m) SafeHash -> Mutable ak (PrimState m) k -> Mutable av (PrimState m) v -> Int -> SafeHash -> k -> v -> m (SafeHash, k, v)
swapEntry hs ks vs i h k v = do
  (h', k', v') <- getEntry hs ks vs i
  setEntry hs ks vs i h k v
  pure (h', k', v')

{-# INLINE withReserved #-}
withReserved ::
     (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v)
  => (MutablePrimArray (PrimState m) SafeHash -> Mutable ak (PrimState m) k -> Mutable av (PrimState m) v -> m a)
  -> HashTable ak av (PrimState m) k v
  -> Int
  -> m a
withReserved continue table !additionalElems = do
  hs <- readMutVar (hashes table)
  ks <- readMutVar (keys table)
  vs <- readMutVar (values table)
  !currentNumItems <- readPrimRef (numItems table)
  let !numBuckets = sizeofMutablePrimArray hs
      !newNumItems = currentNumItems + additionalElems
      !remaining = capacity' numBuckets - currentNumItems
  if (remaining >= additionalElems)
    then continue hs ks vs
    else do
    let !newNumBuckets = capacityFor newNumItems
    hs' <- newPrimArray newNumBuckets
    setPrimArray hs' 0 newNumBuckets emptyHash
    ks' <- stToPrim (Contiguous.new newNumBuckets )
    vs' <- stToPrim (Contiguous.new newNumBuckets)
    writeMutVar (hashes table) hs'
    writeMutVar (keys table) ks'
    writeMutVar (values table) vs'
    when (currentNumItems /= 0) $ do
      let go !i !toInsert = do
            h <- readPrimArray hs i
            if h == emptyHash
              then go (nextBucket numBuckets i) toInsert
              else do
                k <- stToPrim (Contiguous.read ks i)
                v <- stToPrim (Contiguous.read vs i)
                orderedInsert hs' ks' vs' h k v
                when
                  (toInsert > 1)
                  (go (nextBucket numBuckets i) (toInsert - 1))
      start <- firstIdeal hs
      go start currentNumItems
    continue hs' ks' vs'

displacement :: Int -> SafeHash -> Int -> Int
displacement numBuckets hash pos
  | idealPos <= pos = pos - idealPos
  | otherwise = pos + (numBuckets - idealPos)
  where !idealPos = bucketIndex numBuckets hash

{-# INLINABLE findKey #-}
findKey :: (Eq k, Hashable k, PrimMonad m, Contiguous ak, Element ak k) => k -> HashTable ak av (PrimState m) k v -> m (Maybe Int)
findKey k table = do
  hs <- readMutVar (hashes table)
  ks <- readMutVar (keys table)
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
                   k' <- stToPrim (Contiguous.read ks i)
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
orderedInsert :: (PrimMonad m, Contiguous ak, Element ak k, Contiguous av, Element av v) => MutablePrimArray (PrimState m) SafeHash -> Mutable ak (PrimState m) k -> Mutable av (PrimState m) v ->SafeHash -> k -> v -> m ()
orderedInsert hs ks vs h k v = go (bucketIndex numBuckets h)
  where
    !numBuckets = sizeofMutablePrimArray hs
    go !i = do
      h' <- readPrimArray hs i
      if h' == emptyHash
        then setEntry hs ks vs i h k v
        else go (nextBucket numBuckets i)
