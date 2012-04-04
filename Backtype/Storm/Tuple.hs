{-# LANGUAGE DeriveFunctor, GADTs #-}
module Backtype.Storm.Tuple where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Hashable
import Data.IntSet

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

instance Hashable DynamicTuple where
  hashWithSalt s EmptyTuple = s
  hashWithSalt s (DoubleCons d tup) = hashWithSalt s (d, tup)
  hashWithSalt s (BytesCons bs tup) = hashWithSalt s (bs, tup)
  hashWithSalt s (IntCons i tup) = hashWithSalt s (i, tup)
  hashWithSalt s (StringCons txt tup) = hashWithSalt s (txt, tup)

hashFields :: IntSet -> DynamicTuple -> Int
hashFields fields tup = go 0 tup where
  go _ EmptyTuple = 1
  go i tup
    | i `member` fields = case tup of
	BytesCons x tup' -> hash (x, go (i+1) tup')
	IntCons i tup' -> hash (i, go (i+1) tup')
	StringCons s tup' -> hash (s, go (i+1) tup')
	DoubleCons d tup' -> hash (d, go (i+1) tup')
    | otherwise = case tup of
	BytesCons _ tup' -> 31 * go (i+1) tup'
	IntCons _ tup' -> 31 * go (i+1) tup'
	StringCons _ tup' -> 31 * go (i+1) tup'
	DoubleCons _ tup' -> 31 * go (i+1) tup'

class StormTuple a where
  toDynamicTuple :: a -> DynamicTuple
  fromDynamicTuple :: DynamicTuple -> Maybe a

instance StormTuple DynamicTuple where
  toDynamicTuple = id
  fromDynamicTuple = return

data InTuple a = InTuple {
  inContents :: a,
  originStreamId :: String,
  taskId :: Int,
  originComponentId :: String,
  tupleId :: Int}

data OutTuple a = OutTuple {
  outContents :: a,
  anchorIds :: [Int]}