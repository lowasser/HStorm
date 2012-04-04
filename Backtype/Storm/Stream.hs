{-# LANGUAGE GADTs, RecordWildCards #-}
module Backtype.Storm.Stream where

import Backtype.Storm.Tuple

data Stream a = Stream {streamName :: String}