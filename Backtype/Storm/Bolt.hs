{-# LANGUAGE RankNTypes, NamedFieldPuns, RecordWildCards, GADTs #-}
-- The most generic bolt imaginable.
module Backtype.Storm.Bolt where

import Backtype.Storm.Tuple
import Backtype.Storm.Stream

import Control.Monad.IO.Class

import Data.IntSet

import Text.Printf

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
  executeOnTuple :: InTuple DynamicTuple -> BoltM (),
  parHint :: Int}

newtype BoltM a = BoltM {runBoltM :: 
  (forall a . StormTuple a => Stream a -> a -> [AnyDyn InTuple] -> IO ())
  -> (AnyDyn InTuple -> IO ())
  -> (AnyDyn InTuple -> IO ())
  -> IO a}

instance Monad BoltM where
  m >>= k = BoltM $ \ emitFn ackFn failFn -> do
    a <- runBoltM m emitFn ackFn failFn
    runBoltM (k a) emitFn ackFn failFn
  return a = BoltM $ \ _ _ _ -> return a

instance MonadIO BoltM where
  liftIO m = BoltM $ \ _ _ _ -> m
  
emit :: StormTuple a => a -> Stream a -> BoltM ()
emit a stream = emitAnchored a stream []

emitAnchored :: StormTuple a => a -> Stream a -> [AnyDyn InTuple] -> BoltM ()
emitAnchored tuple stream anchors = BoltM $ \ emitFn _ _ -> emitFn stream tuple anchors

ackTuple :: StormTuple a => InTuple a -> BoltM ()
ackTuple tup = BoltM $ \ _ ackFn _ -> ackFn (Dyn tup)

failTuple :: StormTuple a => InTuple a -> BoltM ()
failTuple tup = BoltM $ \ _ _ failFn -> failFn (Dyn tup)

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
	  go (Rule rule:rs) = case fromDynamicTuple inContents of
	    Nothing	-> go rs
	    Just x	-> rule InTuple{inContents = x, ..}
	  go [] = liftIO $ printf "No rules match tuple %d, discarding.\n"

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