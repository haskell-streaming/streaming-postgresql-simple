# Revision history for streaming-postgresql-simple

## 0.2.0.4  -- 2019-11-28

* Support GHC 8.8

## 0.2.0.3  -- 2018-01-02

* Inline the exception handling code. In `streaming-0.1` this was provided by
  `exceptions` instances, but in `streaming-0.2` these instances were removed.
  Rather than rely on the instances, I've just added the implementations
  privately.

## 0.2.0.2  -- 2018-01-02

* Increase upper-bound of `streaming` to allow for 0.2

## 0.2.0.1

Increase upper bound of `base`.

## 0.2.0.0  -- 2017-02-03

### Correctly perform finalisation in `query` functions.

The previous implementation would perform the necessary finalisation only if the
stream was drained. Some handling was in-place such that exceptions wouldn't
cause the stream to end prematurely, but this isn't enough. We now use
`MonadResource` to register an action to drain the stream.

Users should now wrap calls using `query` with `runResourceT`:

```haskell
>>> runResourceT (S.mapM_ print (query c "SELECT * FROM t"))
```

### Correctly deal with transactions in `stream`

`stream` requires a transaction in order to function. If there isn't a
transaction open, `stream` would create one, but if you manually called `commit`
or `rollback` from within the stream, the internal state would become
inconsistent. This would lead to confusing error messages.

We now watch the transaction state as we pull items out from the stream, and
inform the user if the internal state is not what we expected. Further more,
cleanup actions (commit/rolling back the transaction) now only happen if there
is still a transaction open.


## 0.1.0.0  -- 2017-02-02

* First version. Released on an unsuspecting world. Mwahaha.
