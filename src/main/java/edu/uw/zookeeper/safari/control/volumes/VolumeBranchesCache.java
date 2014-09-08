package edu.uw.zookeeper.safari.control.volumes;

import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

/**
 * Assumes that all volume states are watched.
 */
public final class VolumeBranchesCache extends LoggingServiceListener<Service> implements AsyncFunction<VersionedId, VolumeVersion<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        protected Module() {}
        
        @Provides @Singleton
        public VolumeBranchesCache getVolumeBranchesCache(
                final VolumeDescriptorCache descriptors,
                final SchemaClientService<ControlZNode<?>,?> client) {
            return VolumeBranchesCache.listen(
                    descriptors.descriptors().lookup(), 
                    client.materializer().cache(), 
                    descriptors, 
                    client.cacheEvents());
        }
        
        @Override
        protected void configure() {
            bind(Key.get(new TypeLiteral<AsyncFunction<VersionedId, VolumeVersion<?>>>(){}, Volumes.class)).to(VolumeBranchesCache.class);
        }
    }
    
    public static VolumeBranchesCache listen(
            AsyncFunction<Identifier, ZNodePath> idToPath,
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            Service service,
            WatchListeners cacheEvents) {
        final ConcurrentMap<VersionedId, Promise<VolumeVersion<?>>> branches = new MapMaker().softValues().makeMap();
        final Logger logger = LogManager.getLogger(VolumeBranchesCache.class);
        final StateCacheListener callback = StateCacheListener.listen(idToPath, cache, branches, service, cacheEvents, logger);
        final VolumeBranchesCache instance = new VolumeBranchesCache(branches, cache, callback, service, logger);
        service.addListener(instance, MoreExecutors.directExecutor());
        return instance;
    }

    private final ConcurrentMap<VersionedId, Promise<VolumeVersion<?>>> branches;
    private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
    private final FutureCallback<ZNodePath> callback;
    
    protected VolumeBranchesCache(
            ConcurrentMap<VersionedId, Promise<VolumeVersion<?>>> branches,
            LockableZNodeCache<ControlZNode<?>, ?, ?> cache,
            FutureCallback<ZNodePath> callback,
            Service service,
            Logger logger) {
        super(service, logger);
        this.branches = branches;
        this.cache = cache;
        this.callback = callback;
    }

    @Override
    public ListenableFuture<VolumeVersion<?>> apply(VersionedId input)
            throws Exception {
        switch (delegate().state()) {
        case FAILED:
        case STOPPING:
        case TERMINATED:
            return Futures.immediateCancelledFuture();
        default:
            break;
        }
        
        Promise<VolumeVersion<?>> value = branches.get(input);
        if (value == null) {
            value = SettableFuturePromise.create();
            Promise<VolumeVersion<?>> existing = branches.putIfAbsent(input, value);
            if (existing == null) {
                cache.lock().readLock().lock();
                try {
                    ZNodePath path = ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(input.getValue(), input.getVersion());
                    callback.onSuccess(path);
                } finally {
                    cache.lock().readLock().unlock();
                }
            } else {
                value = existing;
            }
        }
        return value;
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        for (Promise<?> value: Iterables.consumingIterable(branches.values())) {
            value.cancel(false);
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        for (Promise<?> value: Iterables.consumingIterable(branches.values())) {
            value.setException(failure);
        }
    }

    protected final static class StateCacheListener implements FutureCallback<ZNodePath> {

        public static StateCacheListener listen(
                AsyncFunction<Identifier, ZNodePath> idToPath,
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                ConcurrentMap<VersionedId, Promise<VolumeVersion<?>>> branches,
                Service service,
                WatchListeners cacheEvents,
                Logger logger) {
            final StateCacheListener instance = create(cache, idToPath, branches, service);
            Watchers.CacheNodeCreatedListener.listen(
                    cache, 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    instance),
                            WatchMatcher.exact(
                                    ControlSchema.Safari.Volumes.Volume.Log.Version.State.PATH, 
                                    Watcher.Event.EventType.NodeDataChanged),
                            logger), 
                    logger);
            return instance;
        }
        
        public static StateCacheListener create(
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                AsyncFunction<Identifier, ZNodePath> idToPath,
                ConcurrentMap<VersionedId, Promise<VolumeVersion<?>>> branches,
                Service service) {
            return new StateCacheListener(new CacheListener(idToPath, branches, service), cache);
        }

        private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
        private final FutureCallback<ControlSchema.Safari.Volumes.Volume.Log.Version.State> callback;
        
        protected StateCacheListener(
                FutureCallback<ControlSchema.Safari.Volumes.Volume.Log.Version.State> callback,
                LockableZNodeCache<ControlZNode<?>,?,?> cache) {
            this.cache = cache;
            this.callback = callback;
        }
        
        /**
         * Assumes cache is read locked.
         */
        @Override
        public void onSuccess(ZNodePath result) {
            final ControlSchema.Safari.Volumes.Volume.Log.Version.State node = (ControlSchema.Safari.Volumes.Volume.Log.Version.State) cache.cache().get(result);
            if ((node != null) && node.data().stamp() > 0L) {
                callback.onSuccess(node);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
        
        private static final class CacheListener extends Watchers.StopServiceOnFailure<ControlSchema.Safari.Volumes.Volume.Log.Version.State> {

            private final ConcurrentMap<VersionedId, Promise<VolumeVersion<?>>> branches;
            private final AsyncFunction<Identifier, ZNodePath> idToPath;
            
            private CacheListener(
                    AsyncFunction<Identifier, ZNodePath> idToPath,
                    ConcurrentMap<VersionedId, Promise<VolumeVersion<?>>> branches,
                    Service service) {
                super(service);
                this.branches = branches;
                this.idToPath = idToPath;
            }
            
            /**
             * Assumes cache is read locked.
             */
            @Override
            public void onSuccess(ControlSchema.Safari.Volumes.Volume.Log.Version.State result) {
                final VersionedId version = result.version().id();
                assert (result.data().stamp() > 0L);
                final Optional<RegionAndLeaves> leaves = Optional.fromNullable(result.data().get());
                Promise<VolumeVersion<?>> value = branches.get(version);
                if (value != null) {
                    if (!value.isDone()) {
                        VolumeBranchListener.forState(version, leaves, idToPath, value);
                    }
                } else {
                    value = SettableFuturePromise.create();
                    VolumeBranchListener.forState(version, leaves, idToPath, value);
                    Object existing = branches.putIfAbsent(version, value);
                    if (existing != null) {
                        // try again
                        value.cancel(false);
                        onSuccess(result);
                    }
                }
            }
        }
    }
}
