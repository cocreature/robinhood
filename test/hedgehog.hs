{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where
import           Control.Monad.IO.Class (MonadIO(..))

import           Prelude hiding (lookup)

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Hedgehog
import qualified Hedgehog.Gen as Gen
import           Hedgehog.Internal.Config
import           Hedgehog.Internal.Runner
import qualified Hedgehog.Range as Range

import           Data.HashTable.RobinHood (IOHashTable)
import qualified Data.HashTable.RobinHood as HashTable

newtype State v =
  State (Maybe (Var (Opaque (IOHashTable Int Int)) v, Map Int Int))
  deriving (Eq, Show)

initialState :: State v
initialState =
  State Nothing

data New (v :: * -> *) =
  New Int
  deriving (Eq, Show)

instance HTraversable New where
  htraverse _ (New size) = pure (New size)

genSize :: MonadGen m => m Int
genSize = do
  mayE <- Gen.maybe (Gen.int (Range.linear 0 10))
  case mayE of
    Nothing -> pure 0
    Just e -> pure (2 ^ e)

new :: (MonadGen n, MonadIO m) => Command n m State
new =
  let gen (State (Just _)) = Nothing
      gen (State Nothing) = Just (New <$> genSize)
      execute (New size) = fmap Opaque . liftIO $ HashTable.new size
   in Command
        gen
        execute
        [Update $ \(State _) (New _) o -> State (Just (o, Map.empty))]

data Lookup (v :: * -> *) =
  Lookup (Var (Opaque (IOHashTable Int Int)) v) Int
  deriving (Eq, Show)

instance HTraversable Lookup where
  htraverse f (Lookup table k) = Lookup <$> htraverse f table <*> pure k

lookup :: (MonadGen n, MonadIO m, MonadTest m) => Command n m State
lookup =
  let gen (State Nothing) = Nothing
      gen (State (Just (table, _))) =
        Just (Lookup table <$> Gen.int (Range.linear (-100) 100))
      execute (Lookup table k) = do
        liftIO $ HashTable.lookup (opaque table) k
   in Command
        gen
        execute
        [ Ensure $ \_s0 (State (Just (_, m))) (Lookup _ k) o ->
            o === Map.lookup k m
        ]

data Insert v =
  Insert (Var (Opaque (IOHashTable Int Int)) v) Int Int
  deriving (Eq, Show)

instance HTraversable Insert where
  htraverse f (Insert table k v) =
    Insert <$> htraverse f table <*> pure k <*> pure v

insert :: (MonadGen n, MonadIO m) => Command n m State
insert =
  let gen (State Nothing) = Nothing
      gen (State (Just (table, _))) =
        Just
          (Insert table <$> Gen.int (Range.linear (-100) 100) <*>
           Gen.int (Range.linear (-100) 100))
      execute (Insert table k v) = liftIO $ HashTable.insert (opaque table) k v
   in Command
        gen
        execute
        [ Update $ \(State (Just (table, m))) (Insert _ k v) _o ->
            State (Just (table, Map.insert k v m))
        ]

data Delete v =
  Delete (Var (Opaque (IOHashTable Int Int)) v) Int
  deriving (Eq, Show)

instance HTraversable Delete where
  htraverse f (Delete table k) = Delete <$> htraverse f table <*> pure k

delete :: (MonadGen n, MonadIO m) => Command n m State
delete =
  let gen (State Nothing) = Nothing
      gen (State (Just (table, _))) =
        Just (Delete table <$> Gen.int (Range.linear (-100) 100))
      execute (Delete table k) = liftIO (HashTable.delete (opaque table) k)
   in Command
        gen
        execute
        [ Update $ \(State (Just (table, m))) (Delete _ k) _o ->
            State (Just (table, Map.delete k m))
        ]

prop_references_sequential :: Property
prop_references_sequential =
  withTests 1000 $
  property $ do
    actions <-
      forAll $
      Gen.sequential
        (Range.linear 1 1000)
        initialState
        [new, lookup, insert, delete]
    executeSequential initialState actions

main :: IO Bool
main =
  checkGroup (RunnerConfig Nothing Nothing (Just Normal)) $$(discover)
