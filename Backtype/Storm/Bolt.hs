-- The most generic bolt imaginable.
module Backtype.Storm.Bolt where

import Backtype.Storm.Tuple
import Backtype.Storm.Stream
import Data.IntSet

data StreamGrouping =
  ShuffleGrouping
  | FieldGrouping IntSet
  | AllGrouping
  | GlobalGrouping
  | NoneGrouping
  | LocalGrouping

data Bolt = Bolt {
  boltName :: String,
  inputStreams :: [(AnyDyn Stream, StreamGrouping)],
  outputStreams :: [AnyDyn Stream],
  tearDown :: IO (),
  executeOnTuple :: (InTuple DynamicTuple -> IO ()),
  parHint :: Int}