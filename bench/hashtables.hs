{-# LANGUAGE GeneralizedNewtypeDeriving #-}
import Data.HashTable.IO as H
import qualified Data.ByteString as BS
import Data.Hashable

newtype Bad = B { unB :: Int }  deriving (Show, Enum, Ord, Eq)
instance Hashable Bad where
    hash (B i) = i
    hashWithSalt _ (B i) = i

main ::  IO ()
main = do
    ht <- H.new :: IO (H.BasicHashTable Bad BS.ByteString)
    let go (B 63000001) = pure ()
        go n = H.insert ht n BS.empty >> go (B $ unB n + 1)
    go (B 1)
