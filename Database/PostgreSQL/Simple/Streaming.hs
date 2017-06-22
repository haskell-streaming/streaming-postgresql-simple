{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |

There are two main ways to fetch data from PostgreSQL in a streaming
fashion:

1. Directly, using 'ResourceT' to manage resources.

2. Using the various bracketed @with*@ functions; these play nicely
   with @ContT@ from "Control.Monad.Trans.Cont" or - if you will be
   running it all directly in IO with no other transformers on the
   stack - the <http://hackage.haskell.org/package/managed managed>
   package.

-}
module Database.PostgreSQL.Simple.Streaming
  ( -- * Obtaining a connection
    withPGConnection

    -- * Queries that return results
  , query
  , withQuery
  , query_
  , withQuery_

    -- ** Queries taking parser as argument
  , queryWith
  , withQueryWith
  , queryWith_
  , withQueryWith_

    -- * Queries that stream results
  , stream
  , withStream
  , streamWithOptions
  , withStreamWithOptions
  , stream_
  , withStream_
  , streamWithOptions_
  , withStreamWithOptions_

    -- ** Queries that stream results taking a parser as an argument
  , streamWith
  , withStreamWith
  , streamWithOptionsAndParser
  , withStreamWithOptionsAndParser
  , streamWith_
  , withStreamWith_
  , streamWithOptionsAndParser_
  , withStreamWithOptionsAndParser_

    -- * Streaming data in and out of PostgreSQL with @COPY@
  , copyIn
  , copyOut
  , withCopyOut

    -- * Re-exported symbols
  , runResourceT
  ) where

import Control.Exception.Safe
       (MonadMask, catch, throwM, mask)
import Control.Monad (unless)
import Control.Monad.Catch (bracket, onException, finally)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (runReaderT)
import Control.Monad.Trans.Resource
       (MonadResource, register, unprotect, runResourceT)
import Control.Monad.Trans.State.Strict (runStateT)
import qualified Data.ByteString as B
import qualified Data.ByteString as B8
import Data.ByteString.Builder
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as LBS
import Data.Foldable (traverse_)
import Data.IORef
import Data.Int (Int64)
import Data.Monoid ((<>))
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.PostgreSQL.Simple
       (Connection, QueryError(..), ResultError(..), ToRow, FromRow,
        formatQuery, FoldOptions(..), FetchQuantity(..), execute_,
        defaultFoldOptions, rollback, connectPostgreSQL, close)
import qualified Database.PostgreSQL.Simple.Copy as Pg
import Database.PostgreSQL.Simple.FromRow (fromRow)
import Database.PostgreSQL.Simple.Internal
       (RowParser(..), runConversion, throwResultError, withConnection,
        newTempName, exec)
import Database.PostgreSQL.Simple.Internal (Row(..))
import Database.PostgreSQL.Simple.Ok (Ok(..), ManyErrors(..))
import Database.PostgreSQL.Simple.Transaction
       (isFailedTransactionError, beginMode, commit)
import Database.PostgreSQL.Simple.TypeInfo (getTypeInfo)
import Database.PostgreSQL.Simple.Types (Query(..))
import Streaming (Stream, unfold)
import Streaming.Internal (Stream(..))
import Streaming.Prelude (Of(..), reread, for, untilRight)
import qualified Streaming.Prelude as S

-- | A bracketed method of obtaining a PostgreSQL connection which
-- will close on an exception.
withPGConnection :: (MonadMask m, MonadIO m) => B.ByteString
                    -> (Connection -> m r) -> m r
withPGConnection connStr = bracket (liftIO (connectPostgreSQL connStr))
                                   (liftIO . close)

{-|

Perform a @SELECT@ or other SQL query that is expected to return results. Uses
PostgreSQL's <https://www.postgresql.org/docs/current/static/libpq-single-row-mode.html single row mode> to
stream results directly from the socket to Haskell.

It is an error to perform another query using the same connection from within
a stream. This applies to both @streaming-postgresql-simple@ and
@postgresql-simple@ itself. If you do need to perform subsequent queries from
within a stream, you should use 'stream', which uses cursors and allows
interleaving of queries.

To demonstrate the problems of interleaving queries, if we run the following:

@
let doQuery c =
      queryWith_ (Pg.fromRow :: RowParser (Pg.Only Int))
                 c
                 "VALUES (1), (2)"
in S.print (S.mapM_ (\_ -> doQuery c) (doQuery c))
@

We will encounter the exception:

@
Exception: QueryError {qeMessage = "another command is already in progress\n", qeQuery = "VALUES (1), (2)"}
@

Exceptions that may be thrown:

* 'Database.PostgreSQL.Simple.FormatError': the query string could not be formatted correctly.

* 'QueryError': the result contains no columns (i.e. you should be
  using 'execute' instead of 'query').

* 'ResultError': result conversion failed.

* 'Database.PostgreSQL.Simple.SqlError':  the postgresql backend returned an error,  e.g.
  a syntax or type error,  or an incorrect table or column name.

-}

query
  :: (ToRow q, FromRow r, MonadResource m)
  => Connection
  -> Query
  -> q
  -> Stream (Of r) m ()
query conn template qs =
  doQuery fromRow conn template =<< liftIO (formatQuery conn template qs)

-- | A variant of 'query' that takes a continuation.
withQuery
  :: (ToRow q, FromRow a, MonadIO m, MonadMask m)
  => Connection
  -> Query
  -> q
  -> (Stream (Of a) m () -> m r)
  -> m r
withQuery = withQueryWith fromRow

-- | A version of 'query' that does not perform query substitution.
query_
  :: (FromRow r, MonadResource m)
  => Connection
  -> Query
  -> Stream (Of r) m ()
query_ conn template@(Query que) = doQuery fromRow conn template que

-- | A version of 'withQuery' that does not perform query substitution.
withQuery_
  :: (FromRow a, MonadIO m, MonadMask m)
  => Connection
  -> Query
  -> (Stream (Of a) m () -> m r)
  -> m r
withQuery_ = withQueryWith_ fromRow

-- | A version of 'query' taking parser as argument.
queryWith
  :: (ToRow q, MonadResource m)
  => RowParser r
  -> Connection
  -> Query
  -> q
  -> Stream (Of r) m ()
queryWith parser conn template qs =
  doQuery parser conn template =<< liftIO (formatQuery conn template qs)

-- | A version of 'withQuery' taking the row parser as an argument.
withQueryWith
  :: (ToRow q, MonadIO m, MonadMask m)
  => RowParser a
  -> Connection
  -> Query
  -> q
  -> (Stream (Of a) m () -> m r)
  -> m r
withQueryWith parser conn template qs f =
  liftIO (formatQuery conn template qs)
  >>= \que -> doWithQuery parser conn template que f

-- | A version of 'query_' taking parser as argument.
queryWith_
  :: MonadResource m
  => RowParser r -> Connection -> Query -> Stream (Of r) m ()
queryWith_ parser conn template@(Query que) = doQuery parser conn template que

-- | A version of 'withQuery_' taking the row parser as an argument.
withQueryWith_
  :: (MonadIO m, MonadMask m)
  => RowParser a
  -> Connection
  -> Query
  -> (Stream (Of a) m () -> m r)
  -> m r
withQueryWith_ parser conn template@(Query que) = doWithQuery parser conn template que

doQuery
  :: (MonadResource m)
  => RowParser r -> Connection -> Query -> B.ByteString -> Stream (Of r) m ()
doQuery parser conn q que = do
  ok <-
    liftIO $
    withConnection conn $ \h ->
      LibPQ.sendQuery h que <* LibPQ.setSingleRowMode h
  unless ok $
    liftIO (withConnection conn LibPQ.errorMessage) >>=
    traverse_ (liftIO . throwM . flip QueryError q . C8.unpack)
  yieldResults `onTermination` drainRemainingResults

  where

    drainRemainingResults :: MonadIO m => m ()
    drainRemainingResults =
      S.effects (results conn)

    yieldResults =
      for (results conn) $ \result -> do
        status <- liftIO (LibPQ.resultStatus result)
        case status of
          LibPQ.EmptyQuery ->
            liftIO $ throwM $ QueryError "query: Empty query" q

          LibPQ.CommandOk ->
            liftIO $ throwM $ QueryError "query resulted in a command response" q

          LibPQ.CopyOut ->
            liftIO $ throwM $ QueryError "query: COPY TO is not supported" q

          LibPQ.CopyIn ->
            liftIO $ throwM $ QueryError "query: COPY FROM is not supported" q

          LibPQ.BadResponse ->
            liftIO (throwResultError "query" result status)

          LibPQ.NonfatalError ->
            liftIO (throwResultError "query" result status)

          LibPQ.FatalError ->
            liftIO (throwResultError "query" result status)

          _ ->
            streamResult conn parser result

-- @do@ prefix because we don't want to call this 'withQuery'
doWithQuery :: (MonadMask m, MonadIO m)
            => RowParser a -> Connection -> Query -> B.ByteString
            -> (Stream (Of a) m () -> m r) -> m r
doWithQuery parser conn q que f = do
  ok <-
    liftIO $
    withConnection conn $ \h ->
      LibPQ.sendQuery h que <* LibPQ.setSingleRowMode h
  if not ok
     then liftIO (withConnection conn LibPQ.errorMessage)
          >>= throwM . flip QueryError q . maybe "Unspecified error" C8.unpack
     else f yieldResults `finally` drainRemainingResults
  where
    drainRemainingResults = S.effects (results conn)

    yieldResults =
      for (results conn) $ \result -> do
        status <- liftIO (LibPQ.resultStatus result)
        case status of
          LibPQ.EmptyQuery ->
            liftIO $ throwM $ QueryError "query: Empty query" q

          LibPQ.CommandOk ->
            liftIO $ throwM $ QueryError "query resulted in a command response" q

          LibPQ.CopyOut ->
            liftIO $ throwM $ QueryError "query: COPY TO is not supported" q

          LibPQ.CopyIn ->
            liftIO $ throwM $ QueryError "query: COPY FROM is not supported" q

          LibPQ.BadResponse ->
            liftIO (throwResultError "query" result status)

          LibPQ.NonfatalError ->
            liftIO (throwResultError "query" result status)

          LibPQ.FatalError ->
            liftIO (throwResultError "query" result status)

          _ ->
            streamResult conn parser result

results :: MonadIO m => Connection -> Stream (Of LibPQ.Result) m ()
results = reread (liftIO . flip withConnection LibPQ.getResult)

streamResult
  :: MonadIO m
  => Connection -> RowParser a -> LibPQ.Result -> Stream (Of a) m ()
streamResult conn parser result = do
  nrows <- liftIO (LibPQ.ntuples result)
  ncols <- liftIO (LibPQ.nfields result)
  unfold
    (\row ->
        if row < nrows
          then do
            r <- liftIO (getRowWith parser row ncols conn result)
            return (Right (r :> succ row))
          else return (Left ()))
    0

-- | Perform a @SELECT@ or other SQL query that is expected to return
-- results. Results are streamed incrementally from the server.
--
-- When dealing with small results that don't require further access
-- to the database it may be simpler (and perhaps faster) to use 'query'
-- instead.
--
-- This is implemented using a database cursor.    As such,  this requires
-- a transaction.   This function will detect whether or not there is a
-- transaction in progress,  and will create a 'ReadCommitted' 'ReadOnly'
-- transaction if needed.   The cursor is given a unique temporary name,
-- so the consumer may itself call 'stream'.
--
-- Due to the dependency on transactions, you must ensure that `commit`
-- or `rollback` aren't called on the connection used to form a stream.
-- Doing so causes the stream cursor to be released, making it impossible
-- to stream more results. If you do perform a commit or rollback,
-- 'stream' will raise an exception indicating that a transaction
-- was aborted.
--
-- If you performing transaction writes in a stream, consider instead
-- using save points, which will nest correctly with 'stream'.
--
-- Exceptions that may be thrown:
--
-- * 'Database.PostgreSQL.Simple.FormatError': the query string could not be formatted correctly.
--
-- * 'QueryError': the result contains no columns (i.e. you should be
--   using 'execute' instead of 'query').
--
-- * 'ResultError': result conversion failed.
--
-- * 'Database.PostgreSQL.Simple.SqlError':  the postgresql backend returned an error,  e.g.
--   a syntax or type error,  or an incorrect table or column name.

stream
  :: (FromRow row, ToRow params, MonadMask m, MonadResource m)
  => Connection -> Query -> params -> Stream (Of row) m ()
stream = streamWith fromRow

-- | A version of 'stream' that uses a continuation to help handle
-- resource allocation.
withStream :: (FromRow a, ToRow params, MonadMask m, MonadIO m)
           => Connection -> Query -> params
           -> (Stream (Of a) m () -> m r) -> m r
withStream = withStreamWith fromRow

streamWithOptions :: (FromRow row,ToRow params,MonadResource m,MonadMask m)
                  => FoldOptions
                  -> Connection
                  -> Query
                  -> params
                  -> Stream (Of row) m ()
streamWithOptions options = streamWithOptionsAndParser options fromRow

-- | A version of 'withStream' taking folding options as an
-- argument.
withStreamWithOptions :: (FromRow a, ToRow params, MonadMask m, MonadIO m)
                      => FoldOptions
                      -> Connection
                      -> Query
                      -> params
                      -> (Stream (Of a) m () -> m r)
                      -> m r
withStreamWithOptions options = withStreamWithOptionsAndParser options fromRow

-- | A version of 'stream' taking a parser as an argument.
streamWith
  :: (ToRow params, MonadMask m, MonadResource m)
  => RowParser row -> Connection -> Query -> params -> Stream (Of row) m ()
streamWith = streamWithOptionsAndParser defaultFoldOptions

-- | A version of 'withStream' taking the row parser as an argument.
withStreamWith
  :: (ToRow params, MonadMask m, MonadIO m)
  => RowParser a -> Connection -> Query -> params
  -> (Stream (Of a) m () -> m r) -> m r
withStreamWith = withStreamWithOptionsAndParser defaultFoldOptions

-- | A version of 'streamWithOptions' taking a parser as an argument.
streamWithOptionsAndParser
  :: (ToRow params, MonadMask m, MonadResource m)
  => FoldOptions
  -> RowParser row
  -> Connection
  -> Query
  -> params
  -> Stream (Of row) m ()
streamWithOptionsAndParser options parser conn template qs = do
  q <- liftIO (formatQuery conn template qs)
  doFold options parser conn (Query q)

-- | A version of 'withStreamWithOptions' taking the row parser as an
-- argument.
withStreamWithOptionsAndParser
  :: (ToRow params, MonadMask m, MonadIO m)
  => FoldOptions
  -> RowParser a
  -> Connection
  -> Query
  -> params
  -> (Stream (Of a) m () -> m r)
  -> m r
withStreamWithOptionsAndParser options parser conn template qs f = do
  q <- liftIO (formatQuery conn template qs)
  withFold options parser conn (Query q) f

-- | A version of 'stream' that does not perform query substitution.
stream_
  :: (FromRow row, MonadMask m, MonadResource m)
  => Connection -> Query -> Stream (Of row) m ()
stream_ = streamWith_ fromRow

-- | A version of 'withStream_' that does not perform query substitution.
withStream_ :: (FromRow a, MonadMask m, MonadIO m)
            => Connection -> Query -> (Stream (Of a) m () -> m r) -> m r
withStream_ = withStreamWith_ fromRow

streamWithOptions_ :: (FromRow row,MonadResource m,MonadMask m)
                   => FoldOptions
                   -> Connection
                   -> Query
                   -> Stream (Of row) m ()
streamWithOptions_ options = streamWithOptionsAndParser_ options fromRow

-- | A version of 'withStream_' that takes the fold options as an argument.
withStreamWithOptions_ :: (FromRow a, MonadMask m, MonadIO m)
                       => FoldOptions
                       -> Connection
                       -> Query
                       -> (Stream (Of a) m () -> m r)
                       -> m r
withStreamWithOptions_ options = withStreamWithOptionsAndParser_ options fromRow

-- | A version of 'stream_' taking a parser as an argument.
streamWith_
  :: (MonadMask m, MonadResource m)
  => RowParser row -> Connection -> Query -> Stream (Of row) m ()
streamWith_ parser conn template = doFold defaultFoldOptions parser conn template

-- | A version of 'withStream_' taking the row parser as an argument.
withStreamWith_ :: (MonadMask m, MonadIO m)
                => RowParser a -> Connection -> Query
                -> (Stream (Of a) m () -> m r) -> m r
withStreamWith_ = withStreamWithOptionsAndParser_ defaultFoldOptions

-- | A version of 'streamWithOptions_' taking a parser as an argument.
streamWithOptionsAndParser_
  :: (MonadMask m, MonadResource m)
  => FoldOptions -> RowParser row -> Connection -> Query -> Stream (Of row) m ()
streamWithOptionsAndParser_ options parser conn template =
  doFold options parser conn template

-- | A version of 'withStreamWithOptions_' taking the fold options and
-- row parser as arguments.
withStreamWithOptionsAndParser_
  :: (MonadMask m, MonadIO m)
  => FoldOptions -> RowParser a -> Connection -> Query
  -> (Stream (Of a) m () -> m r) -> m r
withStreamWithOptionsAndParser_ = withFold

doFold :: forall row m.
          (MonadIO m,MonadMask m,MonadResource m)
       => FoldOptions
       -> RowParser row
       -> Connection
       -> Query
       -> Stream (Of row) m ()
doFold FoldOptions{..} parser conn q = do
    stat <- liftIO (withConnection conn LibPQ.transactionStatus)
    case stat of
      LibPQ.TransIdle    ->
        bracketStream (liftIO (beginMode transactionMode conn))
                      (\_ -> ifInTransaction $ liftIO (commit conn))
                      (\_ -> go `onException` ifInTransaction (liftIO (rollback conn)))
      LibPQ.TransInTrans -> go
      LibPQ.TransActive  -> fail "foldWithOpts FIXME:  PQ.TransActive"
         -- This _shouldn't_ occur in the current incarnation of
         -- the library,  as we aren't using libpq asynchronously.
         -- However,  it could occur in future incarnations of
         -- this library or if client code uses the Internal module
         -- to use raw libpq commands on postgresql-simple connections.
      LibPQ.TransInError -> fail "foldWithOpts FIXME:  PQ.TransInError"
         -- This should be turned into a better error message.
         -- It is probably a bad idea to automatically roll
         -- back the transaction and start another.
      LibPQ.TransUnknown -> fail "foldWithOpts FIXME:  PQ.TransUnknown"
         -- Not sure what this means.
  where
    ifInTransaction m = do
      stat <- liftIO (withConnection conn LibPQ.transactionStatus)
      case stat of
        LibPQ.TransInTrans -> m
        _ -> return ()

    declare = do
        name <- newTempName conn
        _ <- execute_ conn $ mconcat
                 [ "DECLARE ", name, " NO SCROLL CURSOR FOR ", q ]
        return name

    closeConn name =
      ifInTransaction $
        (execute_ conn ("CLOSE " <> name) >> return ()) `catch` \ex ->
            -- Don't throw exception if CLOSE failed because the transaction is
            -- aborted.  Otherwise, it will throw away the original error.
            unless (isFailedTransactionError ex) $ throwM ex

    go :: Stream (Of row) m ()
    go =
      bracketStream (liftIO declare) (liftIO . closeConn) $ \(Query name) ->
        let fetchQ =
              toByteString
                (byteString "FETCH FORWARD " <> intDec chunkSize <>
                 byteString " FROM " <>
                 byteString name)
            fetches = untilRight $ do

              stat <- liftIO (withConnection conn LibPQ.transactionStatus)
              case stat of
                LibPQ.TransInTrans -> return ()
                _ -> fail "Stream transaction prematurely aborted"

              result <- liftIO (exec conn fetchQ)
              status <- liftIO (LibPQ.resultStatus result)
              case status of
                LibPQ.TuplesOk -> do
                  nrows <- liftIO (LibPQ.ntuples result)
                  if nrows > 0
                    then return $ Left result
                    else return $ Right ()
                _ -> liftIO (throwResultError "fold" result status)
        in for fetches (streamResult conn parser)

    -- FIXME: choose the Automatic chunkSize more intelligently
    --   One possibility is to use the type of the results,  although this
    --   still isn't a perfect solution, given that common types (e.g. text)
    --   are of highly variable size.
    --   A refinement of this technique is to pick this number adaptively
    --   as results are read in from the database.
    chunkSize = case fetchQuantity of
                 Automatic   -> 256
                 Fixed n     -> n

withFold :: forall a m r. (MonadIO m, MonadMask m)
         => FoldOptions
         -> RowParser a
         -> Connection
         -> Query
         -> (Stream (Of a) m () -> m r)
         -> m r
withFold FoldOptions{..} parser conn q f = do
  stat <- liftIO (withConnection conn LibPQ.transactionStatus)
  case stat of
    LibPQ.TransIdle    ->
      bracket (liftIO (beginMode transactionMode conn))
              (\_ -> ifInTransaction (liftIO (commit conn)))
              (\_ -> go `finally` ifInTransaction (liftIO (rollback conn)))
    LibPQ.TransInTrans -> go
    LibPQ.TransActive  -> fail "foldWithOpts FIXME:  PQ.TransActive"
       -- This _shouldn't_ occur in the current incarnation of
       -- the library,  as we aren't using libpq asynchronously.
       -- However,  it could occur in future incarnations of
       -- this library or if client code uses the Internal module
       -- to use raw libpq commands on postgresql-simple connections.
    LibPQ.TransInError -> fail "foldWithOpts FIXME:  PQ.TransInError"
       -- This should be turned into a better error message.
       -- It is probably a bad idea to automatically roll
       -- back the transaction and start another.
    LibPQ.TransUnknown -> fail "foldWithOpts FIXME:  PQ.TransUnknown"
       -- Not sure what this means.
  where
    ifInTransaction m = do
      stat <- liftIO (withConnection conn LibPQ.transactionStatus)
      case stat of
        LibPQ.TransInTrans -> m
        _ -> return ()

    declare = do
        name <- newTempName conn
        _ <- execute_ conn $ mconcat
                 [ "DECLARE ", name, " NO SCROLL CURSOR FOR ", q ]
        return name

    closeConn name =
      ifInTransaction $
        (execute_ conn ("CLOSE " <> name) >> return ()) `catch` \ex ->
            -- Don't throw exception if CLOSE failed because the transaction is
            -- aborted.  Otherwise, it will throw away the original error.
            unless (isFailedTransactionError ex) $ throwM ex

    go = bracket (liftIO declare)
                 (liftIO . closeConn)
                 (f . yieldResults)

    yieldResults :: Query -> Stream (Of a) m ()
    yieldResults (Query name) =
      let fetchQ =
            toByteString
              (byteString "FETCH FORWARD " <> intDec chunkSize <>
               byteString " FROM " <>
               byteString name)
          fetches = untilRight $ do
            stat <- liftIO (withConnection conn LibPQ.transactionStatus)
            case stat of
              LibPQ.TransInTrans -> return ()
              _ -> fail "Stream transaction prematurely aborted"
            result <- liftIO (exec conn fetchQ)
            status <- liftIO (LibPQ.resultStatus result)
            case status of
              LibPQ.TuplesOk -> do
                nrows <- liftIO (LibPQ.ntuples result)
                if nrows > 0
                  then return $ Left result
                  else return $ Right ()
              _ -> liftIO (throwResultError "fold" result status)
      in for fetches (streamResult conn parser)

    -- FIXME: choose the Automatic chunkSize more intelligently
    --   One possibility is to use the type of the results,  although this
    --   still isn't a perfect solution, given that common types (e.g. text)
    --   are of highly variable size.
    --   A refinement of this technique is to pick this number adaptively
    --   as results are read in from the database.
    chunkSize = case fetchQuantity of
                 Automatic   -> 256
                 Fixed n     -> n

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

toByteString :: Builder -> B.ByteString
toByteString x = LBS.toStrict (toLazyByteString x)

--------------------------------------------------------------------------------
data Restore m = Unmasked | Masked (forall x . m x -> m x)

liftMask
    :: forall a m r . (MonadIO m)
    => (forall s . ((forall x . m x -> m x) -> m s) -> m s)
    -> ((forall x . Stream (Of a) m x -> Stream (Of a) m x)
        -> Stream (Of a) m r)
    -> Stream (Of a) m r
liftMask maskVariant k = do
    ioref <- liftIO $ newIORef Unmasked

    let -- mask adjacent actions in base monad
        loop :: Stream (Of a) m r -> Stream (Of a) m r
        loop (Step f)   = Step (fmap loop f)
        loop (Return r) = Return r
        loop (Effect m) = Effect $ maskVariant $ \unmaskVariant -> do
            -- stash base's unmask and merge action
            liftIO $ writeIORef ioref $ Masked unmaskVariant
            m >>= chunk >>= return . loop

        -- unmask adjacent actions in base monad
        unmask :: forall q. Stream (Of a) m q -> Stream (Of a) m q
        unmask (Step f)   = Step (fmap unmask f)
        unmask (Return q) = Return q
        unmask (Effect m) = Effect $ do
            -- retrieve base's unmask and apply to merged action
            Masked unmaskVariant <- liftIO $ readIORef ioref
            unmaskVariant (m >>= chunk >>= return . unmask)

        -- merge adjacent actions in base monad
        chunk :: forall s. Stream (Of a) m s -> m (Stream (Of a) m s)
        chunk (Effect m) = m >>= chunk
        chunk s          = return s

    loop $ k unmask

bracketStream
  :: (MonadIO m, MonadMask m, MonadResource m)
  => m a -> (a -> IO ()) -> (a -> Stream (Of b) m c) -> Stream (Of b) m c
bracketStream before after action = liftMask mask $ \restore -> do
    h <- lift before
    r <- restore (action h) `onTermination` after h
    liftIO (after h)
    return r

onTermination :: (Functor f, MonadResource m) => Stream f m a -> IO () -> Stream f m a
m1 `onTermination` io = do
  key <- lift (register io)
  clean key m1
  where
    clean key = loop
      where
        loop str =
          case str of
            Return r -> Effect (unprotect key >> return (Return r))
            Effect m -> Effect (fmap loop m)
            Step f -> Step (fmap loop f)

-- | Issue a @COPY FROM STDIN@ query and stream the results in.
--
-- Note that the data in the stream not need to represent a single row, or
-- even an integral number of rows.
--
-- The stream indicates whether or not the copy was succesful. If the stream
-- terminates with 'Nothing', the copy is succesful. If the stream terminates
-- with 'Just' @error@, the error message will be logged.
--
-- If copying was successful, the number of rows processed is returned.
copyIn
  :: (ToRow params, MonadIO m)
  => Connection
  -> Query
  -> params
  -> Stream (Of B.ByteString) m (Maybe B.ByteString)
  -> m (Maybe Int64)
copyIn conn q params copyData =
  do liftIO (Pg.copy conn q params)
     res <- S.mapM_ (liftIO . Pg.putCopyData conn) copyData
     case res of
       Just e -> liftIO $ do
         Pg.putCopyError conn e
         return Nothing

       Nothing -> liftIO $
         fmap Just (Pg.putCopyEnd conn)

-- | Issue a @COPY TO STDOUT@ query and stream the results. When the stream is
-- drained it returns the total amount of rows returned. Each element in the
-- stream is either exactly one row of the result, or header or footer
-- data depending on format.
copyOut
  :: (MonadIO m, ToRow params)
  => Connection -> Query -> params -> Stream (Of B.ByteString) m Int64
copyOut conn q params = do
  liftIO (Pg.copy conn q params)
  untilRight $ do
    res <- liftIO (Pg.getCopyData conn)
    case res of
      Pg.CopyOutRow bytes -> return (Left bytes)
      Pg.CopyOutDone n -> return (Right n)

-- | A variant of 'copyOut' that takes a continuation to handle the output.
--
-- No extra safety is gained with this function, but it can help
-- chaining together with all the other @with*@ functions.
withCopyOut :: (MonadIO m, ToRow params)
            => Connection -> Query -> params
            -> (Stream (Of B.ByteString) m Int64 -> m r) -> m r
withCopyOut conn q params f = f (copyOut conn q params)
