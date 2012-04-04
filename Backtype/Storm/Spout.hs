{-# LANGUAGE RankNTypes, NamedFieldPuns #-}
module Backtype.Storm.Spout where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad.IO.Class

import Backtype.Storm.Tuple
import Backtype.Storm.Stream

data Spout = Spout {
  spoutName :: String,
  startRunning :: SpoutM (MVar ()),
  spoutOutputs :: [AnyDyn Stream]}

newtype SpoutM a = SpoutM {runSpoutM :: (forall x . StormTuple x => x -> Stream x -> IO ()) -> IO a}

instance Monad SpoutM where
  return = liftIO . return
  m >>= k = SpoutM $ \ emitFn -> do
    a <- runSpoutM m emitFn
    runSpoutM (k a) emitFn

instance MonadIO SpoutM where
  liftIO m = SpoutM $ \ _ -> m

emit :: StormTuple a => a -> Stream a -> SpoutM ()
emit a stream = SpoutM $ \ emitFn -> emitFn a stream

pureSpout :: StormTuple a => String -> Stream a -> [a] -> IO Spout
pureSpout spoutName outStream values = do
  runningVar <- newMVar False
  isDone <- newEmptyMVar
  let startRunning = SpoutM $ \ emitFn -> do
	isRunning <- takeMVar runningVar
	if isRunning then putMVar runningVar True else do
	    forkIO $ mapM_ (`emitFn` outStream) values >> putMVar isDone ()
	    putMVar runningVar True
	return isDone
  return Spout{startRunning, spoutName, spoutOutputs = [Dyn outStream]}