module Backtype.Storm.Topology (
  Topology(..),
  Spout,
  Bolt,
  Stream,
  module Backtype.Storm.Tuple) 
  where

import Backtype.Storm.Bolt
import Backtype.Storm.Stream
import Backtype.Storm.Spout
import Backtype.Storm.Tuple

import Control.Concurrent
import Control.Concurrent.Chan

import Data.Monoid

data Topology = Topology {
  spouts :: [Spout],
  bolts :: [Bolt],
  streams :: [AnyDyn Stream]}

instance Monoid Topology where
  mempty = Topology [] [] []
  Topology sp1 b1 st1 `mappend` Topology sp2 b2 st2 =
    Topology (sp1 ++ sp2) (b1 ++ b2) (st1 ++ st2)
