{-# LANGUAGE TypeInType #-}
{-# LANGUAGE UnboxedTuples #-}
module Data.HashTable
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
import           Data.Foldable
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

-- | Mutable, linearly probed hash table.
--
-- The table capacity will be doubled if more than 3/4 of the buckets
-- are full.
--
-- If less than 1/8 of the buckets are empty (i.e. there are many
-- tombstones), a new hash table of the same capacity will be
-- allocated.
--
-- Note that in the current implementation, the hashtable will never shrink.
data HashTable s k v = HashTable
  { hashes :: {-# UNPACK #-}!(MutVar s (MutablePrimArray s SafeHash))
  , buckets :: {-# UNPACK #-}!(MutVar s (MutableArray s (Bucket k v)))
  , numItems :: {-# UNPACK #-}!(PrimRef s Int)
  , numTombstones :: {-# UNPACK #-}!(PrimRef s Int)
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
  tombstones <- newPrimRef 0
  let !table = HashTable hs bs items tombstones
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
  writePrimRef (numTombstones table) 0

-- | @lookupBucketFor k table@ returns the bucket that @k@ belongs to.
--
-- In addition to that it also has a few side effects:
--
-- * It inserts the hash of @k@ in the table.
--
-- * It decrements the number of tombstones if a tombstone is
--   overwritten.
--
-- * It increments the number of items if the key was not already present.
{-# INLINABLE lookupBucketFor #-}
{-# SCC lookupBucketFor #-}
lookupBucketFor :: (Eq k, Hashable k, PrimMonad m) => k -> HashTable (PrimState m) k v -> m Int
lookupBucketFor a table = do
  !initialHs <- readMutVar (hashes table)
  let !initialNumBuckets = sizeofMutablePrimArray initialHs
  when (initialNumBuckets == 0) $ init table 16
  !hs <- readMutVar (hashes table)
  !bs <- readMutVar (buckets table)
  let !numBuckets = sizeofMutablePrimArray hs
  !start <- bucketIndex table h
  let -- Since the table is not completely empty at this point, there
      -- will be at least one empty bucket which will serve as the
      -- stopping condition.
      go !i !firstTombstone = do
        !bucketHash <- readPrimArray hs i
        if | bucketHash == emptyHash -> do
             modifyPrimRef (numItems table) (+ 1)
             if firstTombstone /= -1
               then do
                 modifyPrimRef (numTombstones table) (subtract 1)
                 writePrimArray hs firstTombstone h
                 pure firstTombstone
               else do
                 writePrimArray hs i h
                 pure i
           | bucketHash == tombstoneHash ->
             go
               (nextBucket numBuckets i)
               (if firstTombstone == -1
                  then i
                  else firstTombstone)
           | bucketHash == h ->
             do Bucket k _ <- readArray bs i
                if k == a
                  then pure i
                  else go (nextBucket numBuckets i) firstTombstone
           | otherwise -> go (nextBucket numBuckets i) firstTombstone
  go start (-1)
  where
    !h = safeHash a

{-# INLINABLE bucketIndex #-}
bucketIndex :: PrimMonad m => HashTable (PrimState m) k v -> SafeHash -> m Int
bucketIndex table (SafeHash h) = do
  numBuckets <- sizeofMutablePrimArray <$> readMutVar (hashes table)
  pure (h .&. (numBuckets - 1))

{-# INLINABLE modifyPrimRef #-}
modifyPrimRef :: (Prim a, PrimMonad m) => PrimRef (PrimState m) a -> (a -> a) -> m ()
modifyPrimRef r f = do
  a <- readPrimRef r
  writePrimRef r (f a)

-- | Insert a new key/value pair in the hash table.
{-# INLINABLE insert #-}
insert :: (Eq k, Hashable k, PrimMonad m) => k -> v -> HashTable (PrimState m) k v -> m ()
insert k v table = do
  i <- lookupBucketFor k table
  bs <- readMutVar (buckets table)
  writeArray bs i (Bucket k v)
  rehash table

-- | Lookup the value at a key in the hash table.
{-# INLINABLE lookup #-}
lookup :: (Eq k, Hashable k, PrimMonad m) => k -> HashTable (PrimState m) k v -> m (Maybe v)
lookup k table = do
  mayI <- findKey k table
  case mayI of
    Nothing -> pure Nothing
    Just i -> do
      bs <- readMutVar (buckets table)
      Bucket _ v <- readArray bs i
      pure (Just v)

-- | Deletes the key from the hashtable (if present).
--
-- Returns the previous value stored at that key.
{-# INLINABLE delete #-}
delete :: (Eq k, Hashable k, PrimMonad m) => k -> HashTable (PrimState m) k v -> m (Maybe v)
delete k table = do
  mayI <- findKey k table
  case mayI of
    Nothing -> pure Nothing
    Just i -> do
      hs <- readMutVar (hashes table)
      writePrimArray hs i tombstoneHash
      bs <- readMutVar (buckets table)
      Bucket _ v <- readArray bs i
      -- We write garbage here to make sure that the value and the key can be GCd.
      writeArray bs i undefined
      modifyPrimRef (numItems table) (subtract 1)
      modifyPrimRef (numTombstones table) (+ 1)
      -- TODO Shrink if there are too many tombstones
      pure (Just v)

{-# INLINABLE findKey #-}
findKey :: (Eq k, Hashable k, PrimMonad m) => k -> HashTable (PrimState m) k v -> m (Maybe Int)
findKey k table = do
  hs <- readMutVar (hashes table)
  bs <- readMutVar (buckets table)
  let !numBuckets = sizeofMutablePrimArray hs
  if numBuckets == 0
    then pure Nothing
    else do
      start <- bucketIndex table h
      let -- If the table is not completely empty there is at least
          -- one empty bucket which will serve as the stopping
          -- condition for this loop
          go !i = do
            bucketHash <- readPrimArray hs i
            if | bucketHash == emptyHash -> pure Nothing
               | bucketHash == tombstoneHash -> go (nextBucket numBuckets i)
               | bucketHash == h ->
                 do Bucket k' _ <- readArray bs i
                    if k == k'
                      then pure (Just i)
                      else go (nextBucket numBuckets i)
               | otherwise -> go (nextBucket numBuckets i)
      go start
  where
    !h = safeHash k

{-# INLINABLE nextBucket #-}
nextBucket :: Int -> Int -> Int
nextBucket numBuckets i = (i + 1) .&. (numBuckets - 1)

{-# INLINABLE rehash #-}
{-# SCC rehash #-}
rehash :: PrimMonad m => HashTable (PrimState m) k v -> m ()
rehash table = do
  hs <- readMutVar (hashes table)
  let !numBuckets = sizeofMutablePrimArray hs
  !numItems' <- readPrimRef (numItems table)
  !numTombstones' <- readPrimRef (numTombstones table)
  let rehash' newSize = do
        bs <- readMutVar (buckets table)
        hs' <- newPrimArray newSize
        bs' <- newArray newSize undefined
        writeMutVar (hashes table) hs'
        writeMutVar (buckets table) bs'
        writePrimRef (numTombstones table) 0
        setPrimArray hs' 0 newSize emptyHash
        let getNextFree !i = do
              hash <- readPrimArray hs' i
              if hash == emptyHash
                then pure i
                else getNextFree (nextBucket newSize i)
        for_ [0::Int .. sizeofMutablePrimArray hs - 1] $ \i -> do
          hash <- readPrimArray hs i
          when (hash /= emptyHash && hash /= tombstoneHash) $ do
            bucket <- readArray bs i
            !j <- bucketIndex table hash
            !nextFree <- getNextFree j
            writePrimArray hs' nextFree hash
            writeArray bs' nextFree bucket
  if | 4 * numItems' > 3 * numBuckets -> rehash' (2 * numBuckets)
     | numBuckets - (numItems' + numTombstones') <= numBuckets `div` 8 -> rehash' numBuckets
     | otherwise -> pure ()
