package edu.uw.zookeeper.safari.control.volumes;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;

import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;

/**
 * Assumes log entries are watched.
 */
public class VolumeEntryResponse extends ToStringListenableFuture<Boolean> {

    public static VolumeEntryResponse voted(
            final VolumeLogEntryPath entry,
            final Materializer<ControlZNode<?>,?> materializer,
            final WatchListeners cacheEvents,
            final Logger logger) {
        return create(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote.class, entry, materializer, cacheEvents, logger);
    }

    public static VolumeEntryResponse committed(
            final VolumeLogEntryPath entry,
            final Materializer<ControlZNode<?>,?> materializer,
            final WatchListeners cacheEvents,
            final Logger logger) {
        return create(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.class, entry, materializer, cacheEvents, logger);
    }
    
    protected static <T extends ControlZNode<Boolean>> VolumeEntryResponse create(
            final Class<T> type,
            final VolumeLogEntryPath entry,
            final Materializer<ControlZNode<?>,?> materializer,
            final WatchListeners cacheEvents,
            final Logger logger) {
        final Promise<Boolean> promise = SettableFuturePromise.create();
        EntryDeletedListener.listen(entry.path(), promise, materializer.cache(), cacheEvents, logger);
        VoteListener.listen(type, entry.path(), promise, materializer, cacheEvents, logger);
        return new VolumeEntryResponse(entry, promise);
    }

    protected final VolumeLogEntryPath path;
    protected final Promise<Boolean> delegate;
    
    protected VolumeEntryResponse(
            VolumeLogEntryPath path,
            Promise<Boolean> delegate) {
        this.path = path;
        this.delegate = delegate;
    }
    
    public VolumeLogEntryPath path() {
        return path;
    }

    @Override
    protected Promise<Boolean> delegate() {
        return delegate;
    }
    
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        return super.toStringHelper(helper.addValue(path));
    }
    
    protected static abstract class CacheListener<T extends ControlZNode<?>> extends Watchers.SetExceptionCallback<T,Boolean> implements FutureCallback<T> {
        
        protected static <T extends ControlZNode<?>, U extends CacheListener<T>> U listen(
                final U instance,
                final LockableZNodeCache<ControlZNode<?>,?,?> cache,
                final WatchListeners cacheEvents,
                final Logger logger) {
            final Watchers.FutureCallbackListener<? extends Watchers.EventToPathCallback<?>> listener = 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                            instance, cache.cache())), 
                            instance.getWatchMatcher(), 
                            logger);
            cacheEvents.subscribe(listener);
            // replay
            cache.lock().readLock().lock();
            try {
                listener.callback().callback().onSuccess(instance.getWatchMatcher().getPath());
            } finally {
                cache.lock().readLock().unlock();
            }
            Watchers.UnsubscribeWhenDone.listen(instance, listener, cacheEvents);
            return instance;
        }
        
        private final WatchMatcher matcher;
        
        private CacheListener(
                WatchMatcher matcher,
                Promise<Boolean> delegate) {
            super(delegate);
            this.matcher = matcher;
        }
        
        public WatchMatcher getWatchMatcher() {
            return matcher;
        }
    }
    
    protected static final class VoteListener<T extends ControlZNode<Boolean>> extends CacheListener<T> {

        public static <T extends ControlZNode<Boolean>> VoteListener<T> listen(
                final Class<T> type,
                final ZNodePath path,
                final Promise<Boolean> promise,
                final Materializer<ControlZNode<?>,?> materializer,
                final WatchListeners cacheEvents,
                final Logger logger) {
            final VoteListener<T> instance = listen(
                    new VoteListener<T>(
                    path.join(materializer.schema().apply(type).parent().name()), 
                    promise), materializer.cache(), cacheEvents, logger);
            @SuppressWarnings("unchecked")
            final FixedQuery<?> query = FixedQuery.forIterable(
                    materializer, 
                    PathToRequests.forRequests(
                            Operations.Requests.sync(), 
                            Operations.Requests.getData().setWatch(true))
                            .apply(instance.getWatchMatcher().getPath()));
            Watchers.Query.call(
                    Watchers.MaybeErrorProcessor.maybeNoNode(), 
                    Watchers.SetExceptionCallback.create(instance.delegate()), 
                    query);
            return instance;
        }
        
        private VoteListener(
                ZNodePath path,
                Promise<Boolean> delegate) {
            super(WatchMatcher.exact(
                    path, 
                    Watcher.Event.EventType.NodeDataChanged),
                delegate);
        }

        @Override
        public void onSuccess(T result) {
            if (!isDone()) {
                Optional<Boolean> value = Optional.fromNullable(((result == null) ? null : result.data().get()));
                if (value.isPresent()) {
                    delegate().set(value.get());
                }
            }
        }
    }
    
    protected static final class EntryDeletedListener extends CacheListener<ControlSchema.Safari.Volumes.Volume.Log.Version.Entry> {

        public static EntryDeletedListener listen(
                final ZNodePath path,
                final Promise<Boolean> promise,
                final LockableZNodeCache<ControlZNode<?>,?,?> cache,
                final WatchListeners cacheEvents,
                final Logger logger) {
            final EntryDeletedListener instance = new EntryDeletedListener(path, promise);
            return listen(instance, cache, cacheEvents, logger);
        }
        
        private EntryDeletedListener(
                ZNodePath path,
                Promise<Boolean> delegate) {
            super(WatchMatcher.exact(
                    path, 
                    Watcher.Event.EventType.NodeDeleted),
                delegate);
        }

        @Override
        public void onSuccess(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry result) {
            if (!isDone()) {
                if (result == null) {
                    delegate().cancel(false);
                }
            }
        }
    }
}
