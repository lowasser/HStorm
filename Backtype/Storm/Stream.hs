{-# LANGUAGE GADTs, RecordWildCards, FlexibleInstances #-}
module Backtype.Storm.Stream where

import Data.Hashable

import Backtype.Storm.Tuple

data Stream a = Stream {streamName :: String}

instance Eq (AnyDyn Stream) where
  Dyn (Stream n1) == Dyn (Stream n2) = n1 == n2

instance Ord (AnyDyn Stream) where
  Dyn (Stream n1) `compare` Dyn (Stream n2) = n1 `compare` n2

instance Hashable (AnyDyn Stream) where
  hash (Dyn (Stream n)) = hash n