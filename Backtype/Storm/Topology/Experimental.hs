{-# LANGUAGE ScopedTypeVariables #-}
module Backtype.Storm.Topology.Experimental where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class

import Backtype.Storm.Topology
import Backtype.Storm.Topology.Builder
import Backtype.Storm.Topology.Emulation
import Backtype.Storm.Tuple
import Backtype.Storm.Tuple.Basic
import Backtype.Storm.Spout
import Backtype.Storm.Stream
import Backtype.Storm.Bolt

import System.IO
import Data.Char

import qualified Data.HashMap.Strict as M

import qualified Data.IntSet as IS

dictionarySpout :: Stream Char -> Stream Int -> IO Spout
dictionarySpout charStream lengthStream = makeSpout
  "dictionary" 
  [Dyn charStream, Dyn lengthStream]
  $ \ emit -> withFile "dictionary.txt" ReadMode $ \ hdl -> do
      let loop = do
	    eof <- hIsEOF hdl
	    unless eof $ do
	      word <- hGetLine hdl
	      mapM_ (`emit` charStream) [toLower c | c <- word, isAsciiLower c || isAsciiUpper c]
	      emit (length word) lengthStream
	      loop
      loop `finally` putStrLn "Done with dictionary"

charBolt :: Stream Char -> Bolt
charBolt charStream =
  mkBolt "character-count-bolt" (M.empty, 0)
    [Rule $ \ (chCounts, z) InTuple{inContents = ch :: Char} -> do
	let chCounts' = M.insertWith (+) ch 1 chCounts
	when (z `mod` 100 == 0) $ liftIO $ print chCounts >> yield
	return (chCounts', z + 1)]
  `withInput` (charStream, FieldGrouping $ IS.singleton 0)
  `withParHint` 15

lengthBolt :: Stream Int -> Bolt
lengthBolt intStream =
  mkBolt "int-count-bolt" (M.empty, 0)
    [Rule $ \ (lenCounts, z) InTuple{inContents = len :: Int} -> do
	let lenCounts' = M.insertWith (+) len 1 lenCounts
	when (z `mod` 100 == 0) $ liftIO $ print lenCounts >> yield
	return (lenCounts', z + 1)]
  `withInput` (intStream, FieldGrouping $ IS.singleton 0)
  `withParHint` 5

main :: IO ()
main = do
  topology <- execBuilderM $ do
    charStream <- declareStream "charStream"
    lenStream <- declareStream "lengthStream"
    declareSpout =<< liftIO (dictionarySpout charStream lenStream)
    declareBolt (charBolt charStream)
    declareBolt (lengthBolt lenStream)
  runTopology topology
  threadDelay (10^8)