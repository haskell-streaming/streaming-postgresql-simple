# Revision history for streaming-postgresql-simple

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

## 0.1.0.0  -- YYYY-mm-dd

* First version. Released on an unsuspecting world.
