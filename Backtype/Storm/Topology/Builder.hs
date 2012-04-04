{-# LANGUAGE NamedFieldPuns, GeneralizedNewtypeDeriving, ScopedTypeVariables #-}
module Backtype.Storm.Topology.Builder (
  BuilderM,
  declareStream,
  declareBolt,
  declareSpout,
  execBuilderM) where

import Control.Monad.Trans.Writer.Lazy
import Control.Monad.IO.Class

import Data.Monoid

import Backtype.Storm.Topology
import Backtype.Storm.Stream
import Backtype.Storm.Bolt

newtype BuilderM a = BuilderM {runBuilderM :: WriterT Topology IO a} deriving (Monad, MonadIO)

declareStream :: forall a . StormTuple a => String -> BuilderM (Stream a)
declareStream streamName = BuilderM $ do
  let theStream = Stream{streamName} :: Stream a
  tell mempty{streams = [Dyn theStream]}
  return theStream

declareBolt :: Bolt -> BuilderM ()
declareBolt bolt = BuilderM $ tell mempty{bolts = [bolt]}

declareSpout :: Spout -> BuilderM ()
declareSpout spout = BuilderM $ tell mempty {spouts = [spout]}

execBuilderM :: BuilderM () -> IO Topology
execBuilderM (BuilderM m) = execWriterT m