{-# LANGUAGE GeneralizedNewtypeDeriving #-}
import Data.HashTable.RobinHood as H
import qualified Data.ByteString as BS
import Data.Hashable

newtype Bad = B { unB :: Int }  deriving (Show, Enum, Ord, Eq)
instance Hashable Bad where
    hash (B i) = i
    hashWithSalt _ (B i) = i

main ::  IO ()
main = do
    ht <- H.new 0 :: IO (H.IOHashTable Bad BS.ByteString)
    let go (B 63000001) = pure ()
        go n = H.insert ht n BS.empty >> go (B $ unB n + 1)
    go (B 1)
