module Backtype.Storm.Topology where

import Backtype.Storm.Bolt
import Backtype.Storm.Stream
import Backtype.Storm.Spout
import Backtype.Storm.Tuple

import Control.Concurrent
import Control.Concurrent.Chan

import Data.Set

data Topology = Topology {
  spouts :: [Spout],
  bolts :: [Bolt],
  streams :: [AnyDyn Stream]}
