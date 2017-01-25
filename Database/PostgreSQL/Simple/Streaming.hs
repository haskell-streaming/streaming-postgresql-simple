{-# LANGUAGE BangPatterns, OverloadedStrings #-}

module Database.PostgreSQL.Simple.Streaming
  ( -- * Queries that return results
    query
  , query_

    -- ** Queries taking parser as argument
  , queryWith
  , queryWith_
  ) where

import Control.Monad.Catch (MonadThrow(..))
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Reader (runReaderT)
import Control.Monad.Trans.State.Strict (runStateT)
import qualified Data.ByteString as B8
import qualified Data.ByteString as B
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.PostgreSQL.Simple
       (Connection, QueryError(..), ResultError(..), ToRow, FromRow,
        formatQuery)
import Database.PostgreSQL.Simple.Internal
       (RowParser(..), runConversion, throwResultError, withConnection)
import Database.PostgreSQL.Simple.Internal (Row(..))
import Database.PostgreSQL.Simple.FromRow (fromRow)
import Database.PostgreSQL.Simple.TypeInfo (getTypeInfo)
import Database.PostgreSQL.Simple.Ok (Ok(..), ManyErrors(..))
import Database.PostgreSQL.Simple.Types (Query(..))
import Streaming (Stream, concats, unfold)
import Streaming.Prelude (Of(..))
import Control.Monad (unless)
import Data.Foldable (traverse_)
import Data.ByteString.Char8 as C8

{-|

Perform a @SELECT@ or other SQL query that is expected to return results. Uses
PostgreSQL's <single row https://www.postgresql.org/docs/current/static/libpq-single-row-mode.html> to
stream results directly from the socket to Haskell.

It is an error to perform another query using the same connection from within
a stream. For example:

@
let stream c =
      queryWith_ (Pg.fromRow :: RowParser (Pg.Only Int))
                 c
                 "VALUES (1), (2)"
in S.print (S.mapM_ (\_ -> stream c) (stream c))
@

Will raise the exception:

@
Exception: QueryError {qeMessage = "another command is already in progress\n", qeQuery = "VALUES (1), (2)"}
@

Exceptions that may be thrown:

* 'FormatError': the query string could not be formatted correctly.

* 'QueryError': the result contains no columns (i.e. you should be
  using 'execute' instead of 'query').

* 'ResultError': result conversion failed.

* 'SqlError':  the postgresql backend returned an error,  e.g.
  a syntax or type error,  or an incorrect table or column name.

-}

query
  :: (ToRow q, FromRow r)
  => Connection
  -> Query
  -> q
  -> Stream (Of r) IO ()
query conn template qs =
  go fromRow conn template =<< liftIO (formatQuery conn template qs)

-- | A version of 'query' that does not perform query substitution.
query_
  :: (FromRow r)
  => Connection
  -> Query
  -> Stream (Of r) IO ()
query_ conn template@(Query que) = go fromRow conn template que

-- | A version of 'query' taking parser as argument.
queryWith
  :: (ToRow q)
  => RowParser r
  -> Connection
  -> Query
  -> q
  -> Stream (Of r) IO ()
queryWith parser conn template qs =
  go parser conn template =<< liftIO (formatQuery conn template qs)

-- | A version of 'query_' taking parser as argument.
queryWith_
  :: RowParser r
  -> Connection
  -> Query
  -> Stream (Of r) IO ()
queryWith_ parser conn template@(Query que) = go parser conn template que

go :: RowParser r -> Connection -> Query -> B.ByteString -> Stream (Of r) IO ()
go parser conn q que =
  concats $ do
    ok <-
      liftIO $
      withConnection conn $ \h ->
        LibPQ.sendQuery h que <* LibPQ.setSingleRowMode h
    unless ok $
      liftIO (withConnection conn LibPQ.errorMessage) >>=
      traverse_ (throwM . flip QueryError q . C8.unpack)
    flip unfold () $ \() -> do
      mres <- (withConnection conn LibPQ.getResult)
      case mres of
        Nothing -> return (Left ())
        Just result -> do
          status <- (LibPQ.resultStatus result)
          case status of
            LibPQ.EmptyQuery -> throwM $ QueryError "query: Empty query" q
            LibPQ.CommandOk ->
              throwM $ QueryError "query resulted in a command response" q
            LibPQ.CopyOut ->
              throwM $ QueryError "query: COPY TO is not supported" q
            LibPQ.CopyIn ->
              throwM $ QueryError "query: COPY FROM is not supported" q
            LibPQ.BadResponse -> (throwResultError "query" result status)
            LibPQ.NonfatalError -> (throwResultError "query" result status)
            LibPQ.FatalError -> (throwResultError "query" result status)
            _ -> do
              nrows <- (LibPQ.ntuples result)
              ncols <- (LibPQ.nfields result)
              return $
                Right $
                unfold
                  (\row ->
                     if row < nrows
                       then do
                         r <- (getRowWith parser row ncols conn result)
                         return (Right (r :> succ row))
                       else return (Left ()))
                  0

--------------------------------------------------------------------------------

--
-- Copied verbatim from postgresql-simple
--

getRowWith :: RowParser r
           -> LibPQ.Row
           -> LibPQ.Column
           -> Connection
           -> LibPQ.Result
           -> IO r
getRowWith parser row ncols conn result = do
  let rw = Row row result
  let unCol (LibPQ.Col x) = fromIntegral x :: Int
  okvc <- runConversion (runStateT (runReaderT (unRP parser) rw) 0) conn
  case okvc of
    Ok (val, col)
      | col == ncols -> return val
      | otherwise -> do
        vals <-
          forM' 0 (ncols - 1) $ \c -> do
            tinfo <- getTypeInfo conn =<< LibPQ.ftype result c
            v <- LibPQ.getvalue result row c
            return (tinfo, fmap ellipsis v)
        throwM
          (ConversionFailed
             (show (unCol ncols) ++ " values: " ++ show vals)
             Nothing
             ""
             (show (unCol col) ++ " slots in target type")
             "mismatch between number of columns to \
                      \convert and number in target type")
    Errors [] -> throwM $ ConversionFailed "" Nothing "" "" "unknown error"
    Errors [x] -> throwM x
    Errors xs -> throwM $ ManyErrors xs

forM' :: (Ord n, Num n) => n -> n -> (n -> IO a) -> IO [a]
forM' lo hi m = loop hi []
  where
    loop !n !as
      | n < lo = return as
      | otherwise = do
           a <- m n
           loop (n-1) (a:as)
{-# INLINE forM' #-}


ellipsis :: B8.ByteString -> B8.ByteString
ellipsis bs
    | B.length bs > 15 = B.take 10 bs `B.append` "[...]"
    | otherwise        = bs
