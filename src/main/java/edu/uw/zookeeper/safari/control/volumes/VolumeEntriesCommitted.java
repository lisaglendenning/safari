package edu.uw.zookeeper.safari.control.volumes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class VolumeEntriesCommitted extends ToStringListenableFuture<Optional<Sequential<String,?>>> implements FutureCallback<Optional<Sequential<String,?>>> {

    /**
     * Assumes that log entries for version are being watched.
     * Assumes that all votes for version have been cached.
     */
    public static VolumeEntriesCommitted create(
            final VersionedId version,
            final LockableZNodeCache<ControlZNode<?>,?,?> cache,
            final WatchListeners cacheEvents) {
        final Logger logger = LogManager.getLogger(VolumeEntriesCommitted.class);
        final ZNodePath path = ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(version.getValue(), version.getVersion());
        final VolumeEntriesCommitted future = LoggingFutureListener.listen(logger, create());
        final Watchers.FutureCallbackListener<? extends Watchers.EventToPathCallback<?>> listener = VotedEntriesCommitted.create(path, future, cache, logger);
        cacheEvents.subscribe(listener);
        // replay
        cache.lock().readLock().lock();
        try {
            listener.callback().callback().onSuccess(path);
        } finally {
            cache.lock().readLock().unlock();
        }
        Watchers.UnsubscribeWhenDone.listen(future, listener, cacheEvents);
        return future;
    }

    protected static VolumeEntriesCommitted create() {
        Promise<Optional<Sequential<String, ?>>> promise = SettableFuturePromise.create();
        return new VolumeEntriesCommitted(promise);
    }
    
    private final Promise<Optional<Sequential<String, ?>>> delegate;
    
    protected VolumeEntriesCommitted(
            Promise<Optional<Sequential<String, ?>>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onSuccess(Optional<Sequential<String, ?>> result) {
        if (!isDone()) {
            delegate().set(result);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        if (!isDone()) {
            delegate().setException(t);
        }
    }

    @Override
    protected Promise<Optional<Sequential<String, ?>>> delegate() {
        return delegate;
    }

    protected static final class VotedEntriesCommitted implements FutureCallback<ZNodePath> {
        
        public static Watchers.FutureCallbackListener<? extends Watchers.EventToPathCallback<?>> create(
                ZNodePath path,
                FutureCallback<Optional<Sequential<String, ?>>> callback,
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                Logger logger) {
            return Watchers.FutureCallbackListener.create(
                    Watchers.EventToPathCallback.create(
                            create(cache.cache(), callback, logger)), 
                    WatchMatcher.exact(
                            path.join(
                                    ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.LABEL).join(
                                            ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.LABEL), 
                            Watcher.Event.EventType.NodeDataChanged), 
                    logger);
        }
        
        protected static VotedEntriesCommitted create(
                NameTrie<ControlZNode<?>> trie,
                FutureCallback<Optional<Sequential<String, ?>>> callback,
                Logger logger) {
            return new VotedEntriesCommitted(trie, callback, logger);
        }
        
        private final Logger logger;
        private final FutureCallback<Optional<Sequential<String, ?>>> callback;
        private final NameTrie<ControlZNode<?>> trie;
        
        private VotedEntriesCommitted(
                NameTrie<ControlZNode<?>> trie,
                FutureCallback<Optional<Sequential<String, ?>>> callback,
                Logger logger) {
            this.logger = logger;
            this.callback = callback;
            this.trie = trie;
        }
        
        /**
         * Assumes result is an existing znode.
         * Assumes cachs is read locked.
         */
        @Override
        public void onSuccess(ZNodePath result) {
            Optional<Sequential<String,?>> committed = Optional.absent();
            ControlZNode<?> node = trie.get(result);
            ControlSchema.Safari.Volumes.Volume.Log.Version version;
            if (node instanceof ControlSchema.Safari.Volumes.Volume.Log.Version) {
                version = (ControlSchema.Safari.Volumes.Volume.Log.Version) node;
            } else {
                version = ((ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit) node).entry().version();
            }
            for (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry: version.entries().values()) {
                if ((entry.vote() != null) && ((entry.commit() == null) || (entry.commit().data().get() == null))) {
                    return;
                }
                if (entry.commit().data().get().booleanValue()) {
                    assert (!committed.isPresent());
                    committed = Optional.<Sequential<String,?>>of(entry.name());
                    logger.info("{} COMMITTED for volume {}", committed.get(), version.id());
                }
            }
            callback.onSuccess(committed);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
}