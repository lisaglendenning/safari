package edu.uw.zookeeper.safari.control.volumes;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

/**
 * Assumes that all volumes and volume paths are watched.
 */
public final class VolumeDescriptorCache extends ServiceListenersService {

    public static Module module() {
        return Module.create();
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        protected Module() {}
        
        @Provides @Singleton
        public VolumeDescriptorCache getVolumeDescriptorCache(
                SchemaClientService<ControlZNode<?>,?> client,
                ServiceMonitor monitor) {
            VolumeDescriptorCache instance = VolumeDescriptorCache.create(
                    client.materializer(), 
                    client.materializer().cache(), 
                    client.cacheEvents(), 
                    ImmutableList.<Service.Listener>of());
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {
        }
    }
    
    public static VolumeDescriptorCache create(
            ClientExecutor<? super Records.Request,?,?> client,
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            WatchListeners cacheEvents,
            Iterable<? extends Service.Listener> listeners) {
        final VolumeDescriptors descriptors = VolumeDescriptors.create();
        final VolumeDescriptorCache instance = new VolumeDescriptorCache(descriptors, listeners);
        for (Pair<? extends FutureCallback<ZNodePath>, WatchMatcher> query: ImmutableList.of(
                Pair.create(
                        new VolumeDeletedListener(descriptors, instance, instance.logger()),
                        WatchMatcher.exact(
                                ControlSchema.Safari.Volumes.Volume.PATH,
                                Watcher.Event.EventType.NodeDeleted)),
                Pair.create(
                        new VolumePathListener(cache, descriptors, instance, instance.logger()),
                        WatchMatcher.exact(
                                ControlSchema.Safari.Volumes.Volume.Path.PATH,
                                Watcher.Event.EventType.NodeDataChanged)))) {
            Watchers.FutureCallbackServiceListener.listen(
                    Watchers.EventToPathCallback.create(
                            query.first()), 
                    instance, 
                    cacheEvents, 
                    query.second(), 
                    instance.logger());
        }
        return instance;
    }
    
    private final VolumeDescriptors descriptors;

    protected VolumeDescriptorCache(
            VolumeDescriptors descriptors,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.descriptors = descriptors;
    }
    
    public VolumeDescriptors descriptors() {
        return descriptors;
    }
    
    @Override
    protected Executor executor() {
        return MoreExecutors.directExecutor();
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        descriptors.clear();
    }
    
    public static final class VolumeDescriptors {
        
        public static VolumeDescriptors create() {
            ConcurrentMap<Identifier, ZNodePath> cache = new MapMaker().makeMap();
            ConcurrentMap<Identifier, Callback> callbacks = new MapMaker().makeMap();
            return new VolumeDescriptors(cache, callbacks);
        }

        private final ConcurrentMap<Identifier, ZNodePath> cache;
        private final ConcurrentMap<Identifier, Callback> callbacks;
        private final CachedFunction<Identifier, ZNodePath> lookup;
        private final Logger logger;
        
        protected VolumeDescriptors(
                ConcurrentMap<Identifier, ZNodePath> cache,
                ConcurrentMap<Identifier, Callback> callbacks) {
            this.logger = LogManager.getLogger(this);
            this.cache = cache;
            this.callbacks = callbacks;
            this.lookup = CachedFunction.create(
                    Functions.forMap(cache), 
                    new AsyncLookup(), 
                    logger);
        }
        
        public Logger logger() {
            return logger;
        }
        
        /**
         * We assume that lookups will eventually be resolved (i.e. non-deleted volumes)
         */
        public CachedFunction<Identifier, ZNodePath> lookup() {
            return lookup;
        }
        
        public boolean put(Identifier id, ZNodePath path) {
            ZNodePath existing = cache.putIfAbsent(id, path);
            if (existing == null) {
                // we assume that everyone can now see the new mapping
                Callback callback = callbacks.remove(id);
                if (callback != null) {
                    callback.run();
                }
                return true;
            } else {
                assert (existing.equals(path));
                return false;
            }
        }
        
        /**
         * We don't remember deleted volumes.
         */
        public boolean remove(Identifier id) {
            Callback callback = callbacks.remove(id);
            if (callback != null) {
                callback.cancel(false);
            }
            ZNodePath path = cache.remove(id);
            return (path != null) || (callback != null);
        }
        
        public void clear() {
            for (Callback callback: Iterables.consumingIterable(callbacks.values())) {
                callback.cancel(false);
            }
            cache.clear();
        }
        
        private final class Callback extends ToStringListenableFuture<ZNodePath> implements Runnable {

            private final Identifier id;
            private final Promise<ZNodePath> promise;
            
            private Callback(
                    Identifier id,
                    Promise<ZNodePath> promise) {
                this.id = id;
                this.promise = promise;
                addListener(this, MoreExecutors.directExecutor());
            }
            
            @Override
            public void run() {
                if (isDone()) {
                    callbacks.remove(id, this);
                } else {
                    ZNodePath value = cache.get(id);
                    if (value != null) {
                        delegate().set(value);
                    }
                }
            }

            @Override
            protected Promise<ZNodePath> delegate() {
                return promise;
            }
        }
        
        private final class AsyncLookup implements AsyncFunction<Identifier, ZNodePath> {

            private AsyncLookup() {}
            
            @Override
            public ListenableFuture<ZNodePath> apply(Identifier input)
                    throws Exception {
                Callback callback = new Callback(input, SettableFuturePromise.<ZNodePath>create());
                Callback existing = callbacks.putIfAbsent(input, callback);
                if (existing == null) {
                    callback.run();
                } else {
                    callback = existing;
                }
                return callback;
            }
        }
    }
    
    protected static abstract class VolumeDescriptorsListener<V> extends Watchers.StopServiceOnFailure<V> {

        protected final VolumeDescriptors descriptors;
        protected final Logger logger;
        
        protected VolumeDescriptorsListener(
                VolumeDescriptors descriptors,
                Service service,
                Logger logger) {
            super(service);
            this.descriptors = descriptors;
            this.logger = logger;
        }
    }
    
    protected static final class VolumeDeletedListener extends VolumeDescriptorsListener<ZNodePath> {

        protected VolumeDeletedListener(
                VolumeDescriptors descriptors,
                Service service,
                Logger logger) {
            super(descriptors, service, logger);
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            final Identifier id = Identifier.valueOf(result.label().toString());
            if (descriptors.remove(id)) {
                logger.info("Volume {} removed", id);
            }
        }
    }
    
    protected static final class VolumePathListener extends VolumeDescriptorsListener<ZNodePath> {

        private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
        
        public VolumePathListener(
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                VolumeDescriptors descriptors,
                Service service,
                Logger logger) {
            super(descriptors, service, logger);
            this.cache = cache;
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            final ControlSchema.Safari.Volumes.Volume.Path node = (ControlSchema.Safari.Volumes.Volume.Path) cache.cache().get(result);
            final Identifier id = node.volume().name();
            final ZNodePath path = node.data().get();
            if (descriptors.put(id, path)) {
                logger.info("Volume {} is at path {}", id, path);
            }
        }
    }
}
