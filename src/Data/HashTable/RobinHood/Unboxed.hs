module Data.HashTable.RobinHood.Unboxed
  ( HashTable
  , IOHashTable
  , new
  , insert
  , lookup
  , delete
  , fromList
  , mapM_
  , size
  , capacity
  ) where

import           Prelude hiding (lookup)

import           Control.Monad.Primitive
import           Data.Hashable
import           Data.Primitive.PrimArray
import           Data.Primitive.Types

import qualified Data.HashTable.RobinHood.Internal as Internal

newtype HashTable s k v = HashTable
  { getInternalTable :: Internal.HashTable PrimArray PrimArray s k v
  } deriving (Eq)

type IOHashTable k v = HashTable (PrimState IO) k v

{-# INLINABLE new #-}
new :: (PrimMonad m, Prim k, Prim v) => Int -> m (HashTable (PrimState m) k v)
new = fmap HashTable . Internal.new

{-# INLINABLE insert #-}
insert :: (Eq k, Hashable k, Prim k, Prim v, PrimMonad m) => HashTable (PrimState m) k v -> k -> v -> m ()
insert (HashTable table) k v = Internal.insert table k v

{-# INLINABLE lookup #-}
lookup :: (Eq k, Hashable k, Prim k, Prim v, PrimMonad m) => HashTable (PrimState m) k v -> k -> m (Maybe v)
lookup (HashTable table) k = Internal.lookup table k

{-# INLINABLE delete #-}
delete :: (Eq k, Hashable k, Prim k, Prim v, PrimMonad m) => HashTable (PrimState m) k v -> k -> m (Maybe v)
delete (HashTable table) k = Internal.delete table k

{-# INLINABLE fromList #-}
fromList :: (Eq k, Hashable k, Prim k, Prim v, PrimMonad m) => [(k, v)] -> m (HashTable (PrimState m) k v)
fromList = fmap HashTable . Internal.fromList

{-# INLINABLE size #-}
size :: PrimMonad m => HashTable (PrimState m) k v -> m Int
size = Internal.size . getInternalTable

{-# INLINABLE capacity #-}
capacity :: PrimMonad m => HashTable (PrimState m) k v -> m Int
capacity = Internal.size . getInternalTable
