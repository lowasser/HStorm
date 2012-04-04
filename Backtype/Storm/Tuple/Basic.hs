module Backtype.Storm.Tuple.Basic where

import Data.ByteString (ByteString)
import Data.Text (Text)

import Backtype.Storm.Tuple

infixr 3 `cons`

class TupleComponent a where
  match :: DynamicTuple -> Maybe (a, DynamicTuple)
  cons :: a -> DynamicTuple -> DynamicTuple

instance TupleComponent Int where
  match (IntCons i tup) = Just (i, tup)
  match _ = Nothing
  cons = IntCons
  
instance TupleComponent Double where
  match (DoubleCons d tup) = Just (d, tup)
  match _ = Nothing
  cons = DoubleCons

instance TupleComponent ByteString where
  match (BytesCons bs tup) = Just (bs, tup)
  match _ = Nothing
  cons = BytesCons

instance TupleComponent Text where
  match (StringCons txt tup) = Just (txt, tup)
  match _ = Nothing
  cons = StringCons

instance StormTuple () where
  fromDynamicTuple EmptyTuple = Just ()
  fromDynamicTuple _ = Nothing
  toDynamicTuple () = EmptyTuple

instance StormTuple Int where
  fromDynamicTuple tup = do
    (i, EmptyTuple) <- match tup
    return i
  toDynamicTuple x = x `cons` EmptyTuple

instance StormTuple Double where
  fromDynamicTuple tup = do
    (i, EmptyTuple) <- match tup
    return i
  toDynamicTuple x = x `cons` EmptyTuple

instance StormTuple ByteString where
  fromDynamicTuple tup = do
    (i, EmptyTuple) <- match tup
    return i
  toDynamicTuple x = x `cons` EmptyTuple

instance StormTuple Text where
  fromDynamicTuple tup = do
    (i, EmptyTuple) <- match tup
    return i
  toDynamicTuple x = x `cons` EmptyTuple

instance (TupleComponent a, TupleComponent b) => StormTuple (a, b) where
  fromDynamicTuple tup = do
    (a, tup') <- match tup
    (b, EmptyTuple) <- match tup'
    return (a, b)
  toDynamicTuple (a, b) = a `cons` b `cons` EmptyTuple

instance (TupleComponent a, TupleComponent b, TupleComponent c) => StormTuple (a, b, c) where
  fromDynamicTuple tup = do
    (a, tup') <- match tup
    (b, tup'') <- match tup'
    (c, EmptyTuple) <- match tup''
    return (a, b, c)
  toDynamicTuple (a, b, c) = a `cons` b `cons` c `cons` EmptyTuple

instance (TupleComponent a, TupleComponent b, TupleComponent c, TupleComponent d) => StormTuple (a, b, c, d) where
  fromDynamicTuple tup = do
    (a, tup') <- match tup
    (b, tup'') <- match tup'
    (c, tup''') <- match tup''
    (d, EmptyTuple) <- match tup'''
    return (a, b, c, d)
  toDynamicTuple (a, b, c, d) = a `cons` b `cons` c `cons` d `cons` EmptyTuple