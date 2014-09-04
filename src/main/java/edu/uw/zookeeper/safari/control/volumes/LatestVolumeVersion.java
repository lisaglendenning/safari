package edu.uw.zookeeper.safari.control.volumes;

import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.DirectoryWatcherService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public final class LatestVolumeVersion extends LoggingServiceListener<Service> implements Function<Identifier, FutureTransition<UnsignedLong>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}
        
        @Provides @Singleton
        public LatestVolumeVersion newLatestVolumeVersion(
                SchemaClientService<ControlZNode<?>,?> client,
                DirectoryWatcherService<ControlSchema.Safari.Volumes> service) {
            return listen(
                    client.materializer().cache(), 
                    service, 
                    client.cacheEvents(), 
                    LogManager.getLogger(LatestVolumeVersion.class));
        }
        
        @Override
        protected void configure() {
            bind(Key.get(new TypeLiteral<Function<Identifier, FutureTransition<UnsignedLong>>>(){}, Volumes.class)).to(LatestVolumeVersion.class);
        }

        @Override
        public Key<? extends Service.Listener> getKey() {
            return Key.get(LatestVolumeVersion.class);
        }
    }
    
    public static LatestVolumeVersion listen(
            LockableZNodeCache<ControlZNode<?>, ?, ?> cache, 
            Service service,
            WatchListeners cacheEvents,
            Logger logger) {
        final ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions = new MapMaker().makeMap();
        DeletedCacheListener.listen(cache, versions, service, cacheEvents, logger);
        LatestCacheListener.listen(cache, versions, service, cacheEvents, logger);
        final LatestVolumeVersion listener = new LatestVolumeVersion(versions, service, logger);
        service.addListener(listener, SameThreadExecutor.getInstance());
        return listener;
    }
    
    private final ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions;
    
    protected LatestVolumeVersion(
            ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
            Service service,
            Logger logger) {
        super(service, logger);
        this.versions = versions;
    }

    @Override
    public FutureTransition<UnsignedLong> apply(Identifier input) {
        if (delegate.isRunning()) {
            FutureTransition<UnsignedLong> value = versions.get(input);
            if (value == null) {
                value = FutureTransition.absent(SettableFuturePromise.<UnsignedLong>create());
                FutureTransition<UnsignedLong> existing = versions.putIfAbsent(input, value);
                if (existing != null) {
                    value = existing;
                }
            }
            return value;
        } else {
            return FutureTransition.absent(Futures.<UnsignedLong>immediateCancelledFuture());
        }
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        for (FutureTransition<UnsignedLong> value: Iterables.consumingIterable(versions.values())) {
            value.getNext().cancel(false);
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        for (FutureTransition<UnsignedLong> value: Iterables.consumingIterable(versions.values())) {
            ((Promise<UnsignedLong>) value.getNext()).setException(failure);
        }
    }
    
    protected static abstract class CacheListener extends Watchers.StopServiceOnFailure<ZNodePath> {

        protected final ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions;
        
        protected CacheListener(
                ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
                Service service) {
            super(service);
            this.versions = versions;
        }
    }
    
    protected final static class LatestCacheListener extends CacheListener {

        public static LatestCacheListener listen(
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
                Service service,
                WatchListeners cacheEvents,
                Logger logger) {
            final LatestCacheListener instance = create(cache, versions, service);
            Watchers.CacheNodeCreatedListener.listen(
                    cache, 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    instance),
                            WatchMatcher.exact(
                                    ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH, 
                                    Watcher.Event.EventType.NodeDataChanged),
                            logger), 
                    logger);
            return instance;
        }
        
        public static LatestCacheListener create(
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
                Service service) {
            return new LatestCacheListener(cache, versions, service);
        }
        
        protected final LockableZNodeCache<ControlZNode<?>,?,?> cache;
        
        protected LatestCacheListener(
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
                Service service) {
            super(versions, service);
            this.cache = cache;
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            final ControlSchema.Safari.Volumes.Volume.Log.Latest latest = (ControlSchema.Safari.Volumes.Volume.Log.Latest) cache.cache().get(result);
            final Identifier id = latest.log().volume().name();
            final UnsignedLong version = latest.data().get();
            FutureTransition<UnsignedLong> value = versions.get(id);
            if (value != null) {
                if (!value.getCurrent().isPresent() || (value.getCurrent().get().longValue() < version.longValue())) {
                    FutureTransition<UnsignedLong> updated = FutureTransition.present(version, SettableFuturePromise.<UnsignedLong>create());
                    boolean replaced = versions.replace(id, value, updated);
                    assert (replaced);
                    ((Promise<UnsignedLong>) value.getNext()).set(version);
                }
            } else {
                value = FutureTransition.present(version, SettableFuturePromise.<UnsignedLong>create());
                FutureTransition<UnsignedLong> existing = versions.putIfAbsent(id, value);
                if (existing != null) {
                    // try again
                    onSuccess(result);
                }
            }
        }
    }
    
    protected final static class DeletedCacheListener extends CacheListener {

        public static DeletedCacheListener listen(
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
                Service service,
                WatchListeners cacheEvents,
                Logger logger) {
            final DeletedCacheListener instance = create(versions, service);
            Watchers.CacheNodeCreatedListener.listen(
                    cache, 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    instance),
                            WatchMatcher.exact(
                                    ControlSchema.Safari.Volumes.Volume.PATH, 
                                    Watcher.Event.EventType.NodeDeleted),
                            logger), 
                    logger);
            return instance;
        }
        
        public static DeletedCacheListener create(
                ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
                Service service) {
            return new DeletedCacheListener(versions, service);
        }
        
        protected DeletedCacheListener(
                ConcurrentMap<Identifier, FutureTransition<UnsignedLong>> versions,
                Service service) {
            super(versions, service);
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            final Identifier id = Identifier.valueOf(result.label().toString());
            FutureTransition<UnsignedLong> value = versions.remove(id);
            if (value != null) {
                value.getNext().cancel(false);
            }
        }
    }
}
