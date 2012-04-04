{-# LANGUAGE DeriveFunctor, GADTs #-}
module Backtype.Storm.Tuple where

import Data.ByteString
import Data.Text

data AnyDyn f where
  Dyn :: StormTuple a => f a -> AnyDyn f

data DynamicTuple =
  EmptyTuple
  | DoubleCons Double DynamicTuple
  | BytesCons ByteString DynamicTuple
  | IntCons Int DynamicTuple
  | StringCons Text DynamicTuple deriving (Eq)

instance Show DynamicTuple where
  show = show . toStrings where
    toStrings EmptyTuple = []
    toStrings (DoubleCons d xs) = show d : toStrings xs
    toStrings (BytesCons bs xs) = show bs : toStrings xs
    toStrings (IntCons i xs) = show i : toStrings xs
    toStrings (StringCons t xs) = show t : toStrings xs

class StormTuple a where
  toDynamicTuple :: a -> DynamicTuple
  fromDynamicTuple :: DynamicTuple -> Maybe a

instance StormTuple DynamicTuple where
  toDynamicTuple = id
  fromDynamicTuple = return

data InTuple a = InTuple {
  tuple :: a,
  originStreamId :: String,
  taskId :: Int,
  originComponentId :: String,
  tupleId :: Integer}