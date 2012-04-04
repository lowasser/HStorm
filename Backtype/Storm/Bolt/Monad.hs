{-# LANGUAGE GeneralizedNewtypeDeriving, RecordWildCards, NamedFieldPuns, GADTs #-}
-- The monad of operations that can be performed within a Bolt.
module Backtype.Storm.Bolt.Monad where

import Control.Monad.IO.Class
import Text.Printf

import Backtype.Storm.Tuple
import Backtype.Storm.Stream
import Backtype.Storm.Bolt

newtype BoltM a = BoltM {runBoltM :: IO a} deriving (Monad, MonadIO)

emit :: StormTuple a => a -> Stream a -> BoltM ()
emit a stream = emitAnchored a stream []

emitAnchored :: StormTuple a => a -> Stream a -> [AnyDyn InTuple] -> BoltM ()
emitAnchored tuple Stream{streamName} anchors = liftIO $
  printf "Emitting tuple %s anchored by tuples %s to stream %s\n"
    (show (toDynamicTuple tuple))
    (show [tupleId | Dyn InTuple{tupleId} <- anchors])
    streamName

ackTuple :: InTuple a -> BoltM ()
ackTuple InTuple{tupleId} = liftIO $ printf "Acking tuple with ID %d\n" tupleId

failTuple :: InTuple a -> BoltM ()
failTuple InTuple{tupleId} = liftIO $ printf "Failing tuple with ID %d\n" tupleId

data BoltRule where
  Rule :: StormTuple a => (InTuple a -> BoltM ()) -> BoltRule

mkBolt :: String -> [BoltRule] -> Bolt
mkBolt boltName rules = Bolt {
  boltName,
  inputStreams = [],
  outputStreams = [],
  parHint = 1,
  tearDown = return (),
  executeOnTuple}
  where executeOnTuple InTuple{..} = go rules where
	  go (Rule rule:rs) = case fromDynamicTuple tuple of
	    Nothing	-> go rs
	    Just x	-> runBoltM (rule InTuple{tuple = x, ..})
	  go [] = printf "No rules match tuple %d, discarding.\n"

infixl 1 `withInput`
infixl 1 `withOutput`
infixl 1 `withParHint`
infixl 1 `withTearDown`

withInput :: StormTuple a => Bolt -> (Stream a, StreamGrouping) -> Bolt
bolt `withInput` (stream, grouping) =
  bolt{inputStreams = (Dyn stream, grouping):inputStreams bolt}

withOutput :: StormTuple a => Bolt -> Stream a -> Bolt
bolt `withOutput` stream = bolt {outputStreams = Dyn stream:outputStreams bolt}

withParHint :: Bolt -> Int -> Bolt
bolt `withParHint` parHint = bolt{parHint}

withTearDown :: Bolt -> IO () -> Bolt
bolt `withTearDown` operation = bolt {tearDown = tearDown bolt >> operation}