module Backtype.Storm.Spout where

data Spout a = Spout {
  spoutName :: String,
  isRunning :: IO Bool,
  startRunning :: IO ()}