{-# LANGUAGE TupleSections, NamedFieldPuns, RecordWildCards, BangPatterns #-}
module Backtype.Storm.Topology.Emulation (runTopology) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad hiding (sequence, forM)
import Control.Monad.IO.Class

import Data.Traversable
import Data.Char
import Data.Unique
import Data.Vector ((!))
import qualified Data.Vector as V
import qualified Data.HashSet as S
import qualified Data.HashMap.Strict as M

import Backtype.Storm.Topology
import Backtype.Storm.Bolt
import Backtype.Storm.Tuple
import Backtype.Storm.Stream
import Backtype.Storm.Spout

import Text.Printf
import Prelude hiding (sequence)

runTopology :: Topology -> IO (MVar ())
runTopology Topology{..} = do
  doneVar <- newEmptyMVar
  streamMap <- M.fromList <$> sequence 
    [(stream,) <$> newChan | stream <- streams]
  spoutLocks <- forM spouts $ \ spout -> runSpout spout ((M.!) streamMap)
  forM_ bolts $ \ bolt -> runBolt bolt ((M.!) streamMap)
  forkIO $ do
    mapM_ takeMVar spoutLocks
    putMVar doneVar ()
  return doneVar

-- Returns a lock that will be released when the spout is finished.
runSpout :: Spout -> (AnyDyn Stream -> Chan (InTuple DynamicTuple)) -> IO (MVar ())
runSpout Spout{..} chanForStream = do
  taskId <- hashUnique <$> newUnique
  let distinctSpouts = S.fromList spoutOutputs
      emitFn tuple stream
	| Dyn stream `S.member` distinctSpouts = do
	    tupleId <- hashUnique <$> newUnique
	    writeChan (chanForStream (Dyn stream)) InTuple{
	      inContents = toDynamicTuple tuple,
	      originStreamId = streamName stream,
	      originComponentId = spoutName,
	      tupleId, taskId}
	| otherwise = fail $ 
	    printf "No stream with name %s registered to spout %s\n"
	      (streamName stream)
	      spoutName
  runSpoutM startRunning emitFn

-- Reads and groups the input streams, and writes to the output streams -- both come from the SAME space!
runBolt :: Bolt -> (AnyDyn Stream -> Chan (InTuple DynamicTuple)) -> IO ()
runBolt bolt@Bolt{..} chanForStream = do
  let emitFn taskId (Dyn stream) OutTuple{..} = do
	tupleId <- hashUnique <$> newUnique
	writeChan (chanForStream (Dyn stream))
	  InTuple{
	    inContents = outContents,
	    originStreamId = streamName stream,
	    originComponentId = boltName,
	    taskId, tupleId}
  !taskInputs <- V.replicateM parHint $ liftM snd $ runBoltTask bolt emitFn
  -- Now, spawn threads to distribute input tuples to the tasks appropriately.
  forM_ inputStreams $ \ (stream, grouping) -> let
    sourceChannel = chanForStream stream
    doShuffleGrouping (ch:chs) = do
      tup <- readChan sourceChannel
      writeChan ch tup
      doShuffleGrouping chs
    in case grouping of
	ShuffleGrouping -> forkIO $ doShuffleGrouping $ cycle $ V.toList taskInputs
	LocalGrouping -> forkIO $ doShuffleGrouping $ cycle $ V.toList taskInputs
	AllGrouping -> forkIO $ forever $ do
	  tup <- readChan sourceChannel
	  V.mapM_ (`writeChan` tup) taskInputs
	GlobalGrouping -> forkIO $ forever $ readChan sourceChannel >>= writeChan (taskInputs ! 0)
	NoneGrouping -> forkIO $ doShuffleGrouping $ cycle $ V.toList taskInputs
	FieldGrouping fields -> forkIO $ forever $ do
	  tup@InTuple{inContents} <- readChan sourceChannel
	  writeChan (taskInputs ! (hashFields fields inContents `mod` V.length taskInputs)) tup

-- | Runs a single instance of a bolt; multiple instances of a bolt may run in parallel.  Returns channels to feed the streams into.
runBoltTask :: Bolt -> (Int -> AnyDyn Stream -> OutTuple DynamicTuple -> IO ()) -> IO (Int, Chan (InTuple DynamicTuple))
runBoltTask Bolt{..} emitFn = do
  -- We don't deal with StreamGroupings here, letting that be managed over all instances of the bolt.
  boltInput <- newChan
  taskId <- hashUnique <$> newUnique
  let ackTuple (Dyn InTuple{tupleId}) = liftIO $ printf "Acking tuple %d\n" tupleId
      failTuple (Dyn InTuple{tupleId}) = liftIO $ printf "Failing tuple %d\n" tupleId
      outStreamSet = S.fromList outputStreams
      emit :: StormTuple a => Stream a -> a -> [AnyDyn InTuple] -> IO ()
      emit stream val anchors
	| Dyn stream `S.member` outStreamSet =
	      emitFn taskId (Dyn stream)
		OutTuple{outContents = toDynamicTuple val, anchorIds = [tupleId | Dyn InTuple{tupleId} <- anchors]}
	| otherwise = fail $ printf "No stream registered to bolt %s with name %s" boltName (streamName stream)
      boltLoop s = do
	tuple <- readChan boltInput
	s' <- runBoltM (executeOnTuple s tuple) emit ackTuple failTuple
	boltLoop s'
  forkIO (boltLoop initTaskState)
  return (taskId, boltInput)