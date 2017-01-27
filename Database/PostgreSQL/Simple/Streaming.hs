{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE BangPatterns, OverloadedStrings, RecordWildCards,
  ScopedTypeVariables #-}

module Database.PostgreSQL.Simple.Streaming
  ( -- * Queries that return results
    query
  , query_

    -- ** Queries taking parser as argument
  , queryWith
  , queryWith_

    -- * Streaming queries with cursors
  , stream
  , stream_

    -- ** Streaming with options
  , FoldOptions(..)
  , FetchQuantity(..)
  , defaultFoldOptions
  , streamWithOptions
  , streamWithOptions_

    -- ** Streaming with options and a @RowParser@
  , streamWithOptionsAndParser
  , streamWithOptionsAndParser_
  ) where

import Control.Monad (unless)
import Control.Monad.Catch
       (MonadThrow(..), MonadMask(..), catch, mask, onException, mask_)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (runReaderT)
import Control.Monad.Trans.Resource
       (MonadResource, register, unprotect)
import Control.Monad.Trans.State.Strict (runStateT)
import qualified Data.ByteString as B
import qualified Data.ByteString as B8
import Data.ByteString.Builder
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as LBS
import Data.Foldable (traverse_)
import Data.IORef
import Data.Monoid ((<>))
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.PostgreSQL.Simple
       (Connection, QueryError(..), ResultError(..), ToRow, FromRow,
        formatQuery, FoldOptions(..), FetchQuantity(..), execute_,
        defaultFoldOptions, rollback)
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
import Streaming (Stream, concats, unfold)
import Streaming.Internal (Stream(..))
import Streaming.Prelude (Of(..))

{-|

Perform a @SELECT@ or other SQL query that is expected to return results. Uses
PostgreSQL's <single row https://www.postgresql.org/docs/current/static/libpq-single-row-mode.html> to
stream results directly from the socket to Haskell.

It is an error to perform another query using the same connection from within
a stream. This applies to both @streaming-postgresql-simple@ and
@postgresql-simple@ itself. If you do need to perform subsequent queries from
within a stream, you should use 'fold'.

For example:

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
  doQuery fromRow conn template =<< liftIO (formatQuery conn template qs)

-- | A version of 'query' that does not perform query substitution.
query_
  :: (FromRow r)
  => Connection
  -> Query
  -> Stream (Of r) IO ()
query_ conn template@(Query que) = doQuery fromRow conn template que

-- | A version of 'query' taking parser as argument.
queryWith
  :: (ToRow q)
  => RowParser r
  -> Connection
  -> Query
  -> q
  -> Stream (Of r) IO ()
queryWith parser conn template qs =
  doQuery parser conn template =<< liftIO (formatQuery conn template qs)

-- | A version of 'query_' taking parser as argument.
queryWith_
  :: MonadIO m
  => RowParser r -> Connection -> Query -> Stream (Of r) m ()
queryWith_ parser conn template@(Query que) = doQuery parser conn template que

doQuery
  :: MonadIO m
  => RowParser r -> Connection -> Query -> B.ByteString -> Stream (Of r) m ()
doQuery parser conn q que =
  concats $ do
    ok <-
      liftIO $
      withConnection conn $ \h ->
        LibPQ.sendQuery h que <* LibPQ.setSingleRowMode h
    unless ok $
      liftIO (withConnection conn LibPQ.errorMessage) >>=
      traverse_ (liftIO . throwM . flip QueryError q . C8.unpack)
    flip unfold () $ \() -> do
      mres <- liftIO (withConnection conn LibPQ.getResult)
      case mres of
        Nothing -> return (Left ())
        Just result -> do
          status <- liftIO (LibPQ.resultStatus result)
          case status of
            LibPQ.EmptyQuery -> do
              getResultUntilNothing conn
              liftIO $ throwM $ QueryError "query: Empty query" q
            LibPQ.CommandOk -> do
              getResultUntilNothing conn
              liftIO $ throwM $ QueryError "query resulted in a command response" q
            LibPQ.CopyOut -> do
              getResultUntilNothing conn
              liftIO $ throwM $ QueryError "query: COPY TO is not supported" q
            LibPQ.CopyIn -> do
              getResultUntilNothing conn
              liftIO $ throwM $ QueryError "query: COPY FROM is not supported" q
            LibPQ.BadResponse -> do
              getResultUntilNothing conn
              liftIO (throwResultError "query" result status)
            LibPQ.NonfatalError -> do
              getResultUntilNothing conn
              liftIO (throwResultError "query" result status)
            LibPQ.FatalError -> do
              getResultUntilNothing conn
              liftIO (throwResultError "query" result status)
            _ -> fmap Right (streamResult conn parser result)

getResultUntilNothing :: MonadIO m => Connection -> m ()
getResultUntilNothing c = liftIO (mask_ go)
  where
    go = maybe (return ()) (const go) =<< withConnection c LibPQ.getResult

streamResult
  :: MonadIO m
  => Connection -> RowParser a -> LibPQ.Result -> m (Stream (Of a) m ())
streamResult conn parser result = do
  nrows <- liftIO (LibPQ.ntuples result)
  ncols <- liftIO (LibPQ.nfields result)
  return $
    unfold
      (\row ->
         if row < nrows
           then do
             r <- liftIO (getRowWith parser row ncols conn result)
             return (Right (r :> succ row))
           else return (Left ()))
      0

stream
  :: (FromRow row, ToRow params, MonadMask m, MonadResource m)
  => Connection -> Query -> params -> Stream (Of row) m ()
stream = streamWithOptions defaultFoldOptions

streamWithOptions
  :: (FromRow row, ToRow params, MonadMask m, MonadResource m)
  => FoldOptions -> Connection -> Query -> params -> Stream (Of row) m ()
streamWithOptions o = streamWithOptionsAndParser o fromRow

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

stream_
  :: (FromRow row, MonadMask m, MonadResource m)
  => Connection -> Query -> Stream (Of row) m ()
stream_ = streamWithOptions_ defaultFoldOptions

streamWithOptions_
  :: (FromRow row, MonadMask m, MonadResource m)
  => FoldOptions -> Connection -> Query -> Stream (Of row) m ()
streamWithOptions_ options conn template = doFold options fromRow conn template

streamWithOptionsAndParser_
  :: (MonadMask m, MonadResource m)
  => FoldOptions -> RowParser row -> Connection -> Query -> Stream (Of row) m ()
streamWithOptionsAndParser_ options parser conn template =
  doFold options parser conn template

doFold
  :: (MonadIO m, MonadMask m, MonadResource m)
  => FoldOptions -> RowParser row -> Connection -> Query -> Stream (Of row) m ()
doFold FoldOptions{..} parser conn q = do
    stat <- liftIO (withConnection conn LibPQ.transactionStatus)
    case stat of
      LibPQ.TransIdle    ->
        bracket (liftIO (beginMode transactionMode conn))
                (\_ -> liftIO (commit conn))
                (\_ -> go `onException` liftIO (rollback conn))
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
    declare = do
        name <- newTempName conn
        _ <- execute_ conn $ mconcat
                 [ "DECLARE ", name, " NO SCROLL CURSOR FOR ", q ]
        return name
    close name = do
        Prelude.putStrLn "Closing"
        (execute_ conn ("CLOSE " <> name) >> return ()) `catch` \ex ->
            -- Don't throw exception if CLOSE failed because the transaction is
            -- aborted.  Otherwise, it will throw away the original error.
            unless (isFailedTransactionError ex) $ throwM ex

    go =
      bracket (liftIO declare) (liftIO . close) $ \(Query name) -> do
        let fetchQ =
              toByteString
                (byteString "FETCH FORWARD " <> intDec chunkSize <>
                 byteString " FROM " <>
                 byteString name)
        concats $
          unfold
            (\() -> do
               result <- liftIO (exec conn fetchQ)
               status <- liftIO (LibPQ.resultStatus result)
               case status of
                 LibPQ.TuplesOk -> do
                   nrows <- liftIO (LibPQ.ntuples result)
                   if nrows > 0
                     then fmap Right (streamResult conn parser result)
                     else return (Left ())
                 _ -> liftIO (throwResultError "fold" result status))
            ()

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
    :: forall m f r . (MonadIO m, Functor f)
    => (forall s . ((forall x . m x -> m x) -> m s) -> m s)
    -> ((forall x . Stream f m x -> Stream f m x)
        -> Stream f m r)
    -> Stream f m r
liftMask maskVariant k = do
    ioref <- liftIO $ newIORef Unmasked

    let -- mask adjacent actions in base monad
        loop :: Stream f m r -> Stream f m r
        loop (Step f)   = Step (fmap loop f)
        loop (Return r) = Return r
        loop (Effect m) = Effect $ maskVariant $ \unmaskVariant -> do
            -- stash base's unmask and merge action
            liftIO $ writeIORef ioref $ Masked unmaskVariant
            m >>= chunk >>= return . loop

        -- unmask adjacent actions in base monad
        unmask :: forall q. Stream f m q -> Stream f m q
        unmask (Step f)   = Step (fmap unmask f)
        unmask (Return q) = Return q
        unmask (Effect m) = Effect $ do
            -- retrieve base's unmask and apply to merged action
            Masked unmaskVariant <- liftIO $ readIORef ioref
            unmaskVariant (m >>= chunk >>= return . unmask)

        -- merge adjacent actions in base monad
        chunk :: forall s. Stream f m s -> m (Stream f m s)
        chunk (Effect m) = m >>= chunk
        chunk s          = return s

    loop $ k unmask

bracket
  :: (Functor f, MonadIO m, MonadMask m, MonadResource m)
  => m a -> (a -> IO ()) -> (a -> Stream f m c) -> Stream f m c
bracket before after action = liftMask mask $ \restore -> do
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
