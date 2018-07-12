{-# LANGUAGE BangPatterns, TypeFamilies #-}

module Main where

import           Gauge

import qualified Data.Vector.Hashtables.Internal as VH
import qualified Data.Vector.Storable.Mutable as VM
import qualified Data.Vector.Storable as V

import qualified Data.Vector.Mutable as BV

import qualified Data.HashTable.IO as H

import qualified Data.HashTable.RobinHood.Unboxed as RobinHood

-- import qualified Data.HashMap.Strict as Map

import           Control.Monad
import           Control.Monad.Primitive
import           Data.IORef

n = 100000 :: Int

------------------------------
-- Insert (preinitialized) ---
------------------------------

htc :: IO ()
htc = do
    ht <- H.newSized n :: IO (H.CuckooHashTable Int Int)
    let go !i | i <= n = H.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

htb :: IO ()
htb = do
    ht <- H.newSized n :: IO (H.BasicHashTable Int Int)
    let go !i | i <= n = H.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

htl :: IO ()
htl = do
    ht <- H.newSized n :: IO (H.LinearHashTable Int Int)
    let go !i | i <= n = H.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

htr :: IO ()
htr = do
    ht <- RobinHood.new n :: IO (RobinHood.IOHashTable Int Int)
    let go !i | i <= n = RobinHood.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

vhtb :: IO ()
vhtb = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) BV.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

vhtk :: IO ()
vhtk = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) VM.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

vht :: IO ()
vht = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) VM.MVector Int VM.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

mvb :: IO ()
mvb = do
    ht <- BV.new (n+1)
    let go !i | i <= n = BV.write ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

mv :: IO ()
mv = do
    ht <- VM.new (n+1)
    let go !i | i <= n = VM.write ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

---------------------------
--- Insert (capacity 0) ---
---------------------------

htcg :: IO ()
htcg = do
    ht <- H.newSized 1 :: IO (H.CuckooHashTable Int Int)
    let go !i | i <= n = H.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

htbg :: IO ()
htbg = do
    ht <- H.newSized 1 :: IO (H.BasicHashTable Int Int)
    let go !i | i <= n = H.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

htrg :: IO ()
htrg = do
    ht <- RobinHood.new 1 :: IO (RobinHood.IOHashTable Int Int)
    let go !i | i <= n = RobinHood.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

vhtbg :: IO ()
vhtbg = do
    ht <- VH.initialize 1 :: IO (VH.Dictionary (PrimState IO) BV.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

vhtkg :: IO ()
vhtkg = do
    ht <- VH.initialize 1 :: IO (VH.Dictionary (PrimState IO) VM.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

vhtg :: IO ()
vhtg = do
    ht <- VH.initialize 1 :: IO (VH.Dictionary (PrimState IO) VM.MVector Int VM.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0

-----------------------
--- Insert (delete) ---
-----------------------

htbd :: IO ()
htbd = do
    ht <- H.newSized n :: IO (H.BasicHashTable Int Int)
    let go !i | i <= n = H.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    let go1 !i | i <= n = H.delete ht i >> go1 (i + 1)
               | otherwise = return ()
    go1 0

htrd :: IO ()
htrd = do
    ht <- RobinHood.new n :: IO (RobinHood.IOHashTable Int Int)
    let go !i | i <= n = RobinHood.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    let go1 !i | i <= n = RobinHood.delete ht i >> go1 (i + 1)
               | otherwise = return ()
    go1 0

vhtbd :: IO ()
vhtbd = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) BV.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    let go1 !i | i <= n = VH.delete ht i >> go1 (i + 1)
               | otherwise = return ()
    go1 0

vhtkd :: IO ()
vhtkd = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) VM.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    let go1 !i | i <= n = VH.delete ht i >> go1 (i + 1)
               | otherwise = return ()
    go1 0

vhtd :: IO ()
vhtd = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) VM.MVector Int VM.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    let go1 !i | i <= n = VH.delete ht i >> go1 (i + 1)
               | otherwise = return ()
    go1 0

------------
--- Find ---
------------

bhfind :: H.BasicHashTable Int Int -> IO Int
bhfind ht = do
    let go !i !s | i <= n = do
                                Just x <- H.lookup ht i
                                go (i + 1) (s + x)
                 | otherwise = return s
    go 0 0

rhfind :: RobinHood.IOHashTable Int Int -> IO Int
rhfind ht = do
    let go !i !s | i <= n = do
                                Just x <- RobinHood.lookup ht i
                                go (i + 1) (s + x)
                 | otherwise = return s
    go 0 0

vhfindb :: VH.Dictionary (PrimState IO) BV.MVector Int BV.MVector Int -> IO Int
vhfindb ht = do
    let go !i !s | i <= n = do
                                x <- VH.findEntry ht i
                                go (i + 1) (s + x)
                 | otherwise = return s
    go 0 0

vhfindk :: VH.Dictionary (PrimState IO) VM.MVector Int BV.MVector Int -> IO Int
vhfindk ht = do
    let go !i !s | i <= n = do
                                x <- VH.findEntry ht i
                                go (i + 1) (s + x)
                 | otherwise = return s
    go 0 0

vhfind :: VH.Dictionary (PrimState IO) VM.MVector Int VM.MVector Int -> IO Int
vhfind ht = do
    let go !i !s | i <= n = do
                                x <- VH.findEntry ht i
                                go (i + 1) (s + x)
                 | otherwise = return s
    go 0 0

bh :: IO (H.BasicHashTable Int Int)
bh = do
    ht <- H.newSized n :: IO (H.BasicHashTable Int Int)
    let go !i | i <= n = H.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    return ht

rh :: IO (RobinHood.IOHashTable Int Int)
rh = do
    ht <- RobinHood.new n :: IO (RobinHood.IOHashTable Int Int)
    let go !i | i <= n = RobinHood.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    return ht

vhb :: IO (VH.Dictionary (PrimState IO) BV.MVector Int BV.MVector Int)
vhb = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) BV.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    return ht

vhk :: IO (VH.Dictionary (PrimState IO) VM.MVector Int BV.MVector Int)
vhk = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) VM.MVector Int BV.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    return ht

vh :: IO (VH.Dictionary (PrimState IO) VM.MVector Int VM.MVector Int)
vh = do
    ht <- VH.initialize n :: IO (VH.Dictionary (PrimState IO) VM.MVector Int VM.MVector Int)
    let go !i | i <= n = VH.insert ht i i >> go (i + 1)
              | otherwise = return ()
    go 0
    return ht

fvh :: IO (VH.FrozenDictionary V.Vector Int V.Vector Int)
fvh = do
    h <- vh
    c <- VH.clone h
    VH.unsafeFreeze c

main :: IO ()
main = do
  bh' <- bh
  rh' <- rh
  vhb' <- vhb
  vhk' <- vhk
  vh' <- vh
  fvh' <- fvh
  defaultMain
    [ bgroup
        "insert (preinitialized capacity)"
        [ bench "hashtables cuckoo" $ nfIO htc
        , bench "hashtables basic" $ nfIO htb
        , bench "hashtables linear" $ nfIO htl
        , bench "robinhood" $ nfIO htr
        , bench "vector-hashtables boxed   keys, boxed   values" $ nfIO vhtb
        , bench "vector-hashtables unboxed keys, boxed   values" $ nfIO vhtk
        , bench "vector-hashtables unboxed keys, unboxed values" $ nfIO vht
        , bench "mutable vector boxed" $ nfIO mvb
        , bench "mutable vector" $ nfIO mv
        ]
    , bgroup
        "insert (capacity 0)"
        [ bench "hashtables cuckoo" $ nfIO htcg
        , bench "hashtables basic" $ nfIO htbg
        , bench "robinhood" $ nfIO htrg
        , bench "vector-hashtables boxed   keys, boxed   values" $ nfIO vhtbg
        , bench "vector-hashtables unboxed keys, boxed   values" $ nfIO vhtkg
        , bench "vector-hashtables unboxed keys, unboxed values" $ nfIO vhtg
        ]
    , bgroup
        "insert (delete)"
        [ bench "hashtables basic" $ nfIO htbd
        , bench "robinhood" $ nfIO htrd
        , bench "vector-hashtables boxed   keys, boxed   values" $ nfIO vhtbd
        , bench "vector-hashtables unboxed keys, boxed   values" $ nfIO vhtkd
        , bench "vector-hashtables unboxed keys, unboxed values" $ nfIO vhtd
        ]
    , bgroup
        "find"
        [ bench "hashtables basic" $ nfIO (bhfind bh')
        , bench "robinhood" $ nfIO (rhfind rh')
        , bench "vector-hashtables boxed   keys, boxed   values" $ nfIO (vhfindb vhb')
        , bench "vector-hashtables unboxed keys, boxed   values" $ nfIO (vhfindk vhk')
        , bench "vector-hashtables unboxed keys, unboxed values" $ nfIO (vhfind vh')
        ]
    ]
