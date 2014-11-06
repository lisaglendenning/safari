package edu.uw.zookeeper.safari.control.volumes;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.LockingCachedFunction;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.OptionalNode;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndBranches;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

public final class LatestVolumeCache extends ServiceListenersService {

    public static Module module() {
        return Module.create();
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        protected Module() {}
        
        @Provides @Singleton
        public LatestVolumeCache getLatestVolumeCache(
                VolumeDescriptorCache descriptors,
                SchemaClientService<ControlZNode<?>,?> client,
                ServiceMonitor monitor) {
            LatestVolumeCache instance = LatestVolumeCache.create(
                    descriptors.descriptors().lookup(), 
                    client.materializer().cache(), 
                    client.cacheEvents(), 
                    ImmutableList.<Service.Listener>of());
            monitor.add(instance);
            return instance;
        }
        
        @Provides @Volumes
        public AsyncFunction<Identifier, VolumeVersion<?>> getIdToVolume(
                LatestVolumeCache volumes) {
            return volumes.idToVolume();
        }
        
        @Provides @Volumes
        public AsyncFunction<ZNodePath, AssignedVolumeBranches> getPathToVolume(
                LatestVolumeCache volumes) {
            return volumes.pathToVolume();
        }

        @Override
        protected void configure() {
        }
    }
    
    public static LatestVolumeCache create(
            AsyncFunction<Identifier, ZNodePath> idToPath,
            LockableZNodeCache<ControlZNode<?>,?,?> cache,
            WatchListeners cacheEvents,
            Iterable<? extends Service.Listener> listeners) {
        LatestVolumeCache instance = new LatestVolumeCache(idToPath, listeners);
        VolumeDeletedListener deleted = instance.new VolumeDeletedListener();
        Watchers.CacheNodeCreatedListener.listen(
                cache, 
                instance, 
                cacheEvents,
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(deleted), 
                        deleted.getWatchMatcher(), instance.logger()), 
                        instance.logger());
        VolumeStateListener state = instance.new VolumeStateListener();
        Watchers.CacheNodeCreatedListener.listen(
                cache, 
                instance, 
                cacheEvents,
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        state, cache.cache())), 
                            state.getWatchMatcher(), instance.logger()), 
                        instance.logger());
        VolumeLatestListener latest = instance.new VolumeLatestListener();
        Watchers.CacheNodeCreatedListener.listen(
                cache, 
                instance, 
                cacheEvents,
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        latest, cache.cache())), 
                        latest.getWatchMatcher(), instance.logger()), 
                        instance.logger());
        return instance;
    }

    private final ReentrantReadWriteLock lock;
    private final VolumeById byId;
    private final VolumeByPath byPath;
    private final VolumeBranchListeners callbacks;
    
    protected LatestVolumeCache(
            final AsyncFunction<Identifier, ZNodePath> idToPath,
            final Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.lock = new ReentrantReadWriteLock(true);
        this.byId = new VolumeById();
        this.byPath = new VolumeByPath();
        this.callbacks = new VolumeBranchListeners(idToPath);
    }
    
    public LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume() {
        return byId.lookup();
    }

    public AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume() {
        return byPath.lookup();
    }
    
    public Set<VolumeVersion<?>> volumes() {
        lock.readLock().lock();
        try {
            return ImmutableSet.copyOf(byId.cache.values());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    protected boolean add(VolumeVersion<?> volume) {
        lock.writeLock().lock();
        try {
            boolean added = byId.add(volume);
            if (added) {
                byPath.add(volume);
            }
            return added;
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected boolean remove(Identifier id) {
        lock.writeLock().lock();
        try {
            logger.info("Removing volume {}", id);
            VolumeVersion<?> removed = byId.remove(id);
            if (removed != null) {
                byPath.remove(removed.getDescriptor().getPath(), id);
            }
            callbacks.remove(id);
            return (removed != null);
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected void clear() {
        lock.writeLock().lock();
        try {
            byPath.clear();
            byId.clear();
            callbacks.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    protected Executor executor() {
        return MoreExecutors.directExecutor();
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        clear();
    }
    
    protected abstract class CacheListener<V> extends Watchers.StopServiceOnFailure<V,LatestVolumeCache> {

        private final WatchMatcher matcher;
        
        protected CacheListener(WatchMatcher matcher) {
            super(LatestVolumeCache.this);
            this.matcher = matcher;
        }
        
        public WatchMatcher getWatchMatcher() {
            return matcher;
        }
    }

    protected final class VolumeDeletedListener extends CacheListener<ZNodePath> {

        public VolumeDeletedListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.PATH,
                    Watcher.Event.EventType.NodeDeleted));
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            final Identifier volume = Identifier.valueOf(result.label().toString());
            remove(volume);
        }
    }
    
    protected final class VolumeLatestListener extends CacheListener<ControlSchema.Safari.Volumes.Volume.Log.Latest> {

        public VolumeLatestListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH,
                    Watcher.Event.EventType.NodeDataChanged));
        }
        
        @Override
        public void onSuccess(ControlSchema.Safari.Volumes.Volume.Log.Latest result) {
            if ((result != null) && (result.data().get() != null)) {
                lock.writeLock().lock();
                try {
                    final UnsignedLong version = result.data().get();
                    final Identifier volume = result.log().volume().name();
                    VolumeVersion<?> existing = byId.cache.get(volume);
                    if ((existing == null) || (existing.getState().getVersion().longValue() < version.longValue())) {
                        final ControlSchema.Safari.Volumes.Volume.Log.Version versionNode = result.log().version(version);
                        if (versionNode != null) {
                            final ControlSchema.Safari.Volumes.Volume.Log.Version.State stateNode = versionNode.state();
                            if ((stateNode != null) && (stateNode.data().stamp() > 0L)) {
                                callbacks.add(volume, version, Optional.fromNullable(stateNode.data().get()));
                            }
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }
    
    protected final class VolumeStateListener extends CacheListener<ControlSchema.Safari.Volumes.Volume.Log.Version.State> {

        public VolumeStateListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.Log.Version.State.PATH, 
                    Watcher.Event.EventType.NodeDataChanged));
        }
        
        @Override
        public void onSuccess(ControlSchema.Safari.Volumes.Volume.Log.Version.State result) {
            if ((result != null) && (result.data().get() != null)) {
                lock.writeLock().lock();
                try {
                    final UnsignedLong version = result.version().name();
                    final Identifier volume = result.version().log().volume().name();
                    ControlSchema.Safari.Volumes.Volume.Log.Latest latestNode = 
                            result.version().log().latest();
                    if ((latestNode != null) && version.equals(latestNode.data().get())) {
                        VolumeVersion<?> existing = byId.cache.get(volume);
                        if ((existing == null) || (existing.getState().getVersion().longValue() < version.longValue())) {
                            callbacks.add(volume, version, Optional.fromNullable(result.data().get()));
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }
    
    protected final class VolumeById {
    
        private final Map<Identifier, VolumeVersion<?>> cache;
        private final Map<Identifier, VolumeLookup> lookups;
        private final LockingCachedFunction<Identifier, VolumeVersion<?>> lookup;
        
        public VolumeById() {
            this.cache = Maps.newHashMap();
            this.lookups = Maps.newHashMap();
            final Function<Identifier, VolumeVersion<?>> cached = 
                new Function<Identifier, VolumeVersion<?>>() {
                    @Override
                    public @Nullable
                    VolumeVersion<?> apply(Identifier id) {
                        lock.readLock().lock();
                        try {
                            // TODO is it really safe to access HashMap.get() concurrently?
                            return cache.get(id);
                        } finally {
                            lock.readLock().unlock();
                        }
                    }
                };
            this.lookup = LockingCachedFunction.create(
                    lock.writeLock(),
                    cached, 
                    new AsyncFunction<Identifier, VolumeVersion<?>>() {
                        @Override
                        public ListenableFuture<VolumeVersion<?>> apply(Identifier volume) {
                            lock.writeLock().lock();
                            try {
                                VolumeVersion<?> value = cache.get(volume);
                                if (value != null) {
                                    return Futures.<VolumeVersion<?>>immediateFuture(value);
                                }
                                VolumeLookup updater = lookups.get(value);
                                if (updater != null) {
                                    return updater;
                                }
                                updater = new VolumeLookup(volume);
                                lookups.put(volume, updater);
                                return updater;
                            } finally {
                                lock.writeLock().unlock();
                            }
                        }
                    },
                    logger);
        }
        
        public LockingCachedFunction<Identifier, VolumeVersion<?>> lookup() {
            return lookup;
        }
        
        /**
         * Assumes write lock is held.
         */
        public boolean add(VolumeVersion<?> volume) {
            final Identifier id = volume.getDescriptor().getId();
            VolumeVersion<?> prev = cache.get(id);
            if (prev != null) {
                assert (prev.getDescriptor().equals(volume.getDescriptor()));
                if (prev.getState().getVersion().longValue() >= volume.getState().getVersion().longValue()) {
                    volume = prev;
                }
            }
            if (volume != prev) {
                logger.debug("Updating {} to {}", prev, volume);
                cache.put(id, volume);
            }
            VolumeLookup future = lookups.remove(id);
            if (future != null) {
                future.set(volume);
            }
            return true;
        }
    
        /**
         * Assumes write lock is held.
         */
        public VolumeVersion<?> remove(Identifier volume) {
            VolumeLookup future = lookups.remove(volume);
            if (future != null) {
                future.cancel(false);
            }
            VolumeVersion<?> removed = cache.remove(volume);
            if (removed != null) {
                logger.debug("Removed {}", removed);
            }
            return removed;
        }
    
        /**
         * Assumes write lock is held.
         */
        public void clear() {
            for (ListenableFuture<?> future: Iterables.consumingIterable(lookups.values())) {
                future.cancel(true);
            }
            cache.clear();
        }
        
        protected final class VolumeLookup extends PromiseTask<Identifier, VolumeVersion<?>> implements Runnable {
    
            public VolumeLookup(Identifier volume) {
                this(volume, SettableFuturePromise.<VolumeVersion<?>>create());
            }
            
            public VolumeLookup(
                    Identifier volume,
                    Promise<VolumeVersion<?>> delegate) {
                super(volume, delegate);
                addListener(this, MoreExecutors.directExecutor());
            }
    
            @Override
            public void run() {
                if (isDone()) {
                    lock.writeLock().lock();
                    try {
                        if (lookups.get(task()) == this) {
                            lookups.remove(task());
                        }
                        if (!isCancelled()) {
                            VolumeVersion<?> volume;
                            try {
                                volume = get();
                            } catch (InterruptedException e) {
                                throw Throwables.propagate(e);
                            } catch (ExecutionException e) {
                                return;
                            }
                            LatestVolumeCache.this.add(volume);
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            }
        }
    }

    protected final class VolumeBranchListeners {
    
        private final AsyncFunction<Identifier, ZNodePath> idToPath;
        private final ConcurrentMap<VersionedId, VolumeBranchCallback> listeners;
        
        public VolumeBranchListeners(AsyncFunction<Identifier, ZNodePath> idToPath) {
            this.idToPath = idToPath;
            this.listeners = new MapMaker().makeMap();
        }
        
        public void add(
                final Identifier id, 
                final UnsignedLong version, 
                final Optional<RegionAndLeaves> state) {
            final VersionedId k = VersionedId.valueOf(version, id);
            if (!listeners.containsKey(k)) {
                VolumeBranchCallback callback = new VolumeBranchCallback(k, VolumeBranchListener.forState(k, state, idToPath));
                if (listeners.putIfAbsent(k, callback) == null) {
                    callback.get().addListener(callback, MoreExecutors.directExecutor());
                }
            }
        }
    
        public void remove(final Identifier id) {
            Iterator<VersionedId> keys = listeners.keySet().iterator();
            while (keys.hasNext()) {
                VersionedId next = keys.next();
                if (next.getValue().equals(id)) {
                    VolumeBranchCallback callback = listeners.remove(next);
                    if (callback != null) {
                        callback.get().cancel(false);
                    }
                }
            }
        }
        
        public void clear() {
            for (VolumeBranchCallback callback: Iterables.consumingIterable(listeners.values())) {
                callback.get().cancel(false);
            }
        }
        
        protected class VolumeBranchCallback implements Runnable {
            
            private final VersionedId version;
            private final ListenableFuture<? extends VolumeVersion<?>> future;
            
            public VolumeBranchCallback(
                    VersionedId version,
                    ListenableFuture<? extends VolumeVersion<?>> future) {
                this.version = version;
                this.future = future;
            }
            
            public ListenableFuture<? extends VolumeVersion<?>> get() {
                return future;
            }
            
            @Override
            public void run() {
                if (future.isDone()) {
                    listeners.remove(version, this);
                    if (!future.isCancelled()) {
                        VolumeVersion<?> volume;
                        try {
                            volume = future.get();
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        } catch (ExecutionException e) {
                            // TODO 
                            return;
                        }
                        LatestVolumeCache.this.add(volume);
                    }
                }
            }
        }
    }

    protected final class VolumeByPath {

        private final VolumeOverlay overlay;
        private final NameTrie<OptionalNode<Promise<AssignedVolumeBranches>>> lookups;
        private final LockingCachedFunction<ZNodePath, AssignedVolumeBranches> lookup;
        
        public VolumeByPath() {
            this.overlay = VolumeOverlay.empty();
            this.lookups = SimpleLabelTrie.forRoot(OptionalNode.<Promise<AssignedVolumeBranches>>root());
            this.lookup = LockingCachedFunction.create(
                    lock.writeLock(),
                    new Function<ZNodePath, AssignedVolumeBranches>() {
                        @Override
                        public @Nullable
                        AssignedVolumeBranches apply(ZNodePath path) {
                            lock.readLock().lock();
                            try {
                                StampedValue<Identifier> id = overlay.apply(path);
                                if (id != null) {
                                    AssignedVolumeBranches volume = (AssignedVolumeBranches) byId.cache.get(id.get());
                                    if ((volume != null) && AssignedVolumeBranches.contains(volume, path)) {
                                        return volume;
                                    }
                                }
                                return null;
                            } finally {
                                lock.readLock().unlock();
                            }
                        }
                    }, 
                    new AsyncFunction<ZNodePath, AssignedVolumeBranches>() {
                        @Override
                        public ListenableFuture<AssignedVolumeBranches> apply(ZNodePath path) {
                            lock.writeLock().lock();
                            try {
                                OptionalNode<Promise<AssignedVolumeBranches>> node = OptionalNode.putIfAbsent(lookups, path);
                                if (!node.isPresent() || node.get().isDone()) {
                                    node.set(LoggingFutureListener.listen(logger, SettableFuturePromise.<AssignedVolumeBranches>create()));
                                    new PathLookupListener(node);
                                }
                                return node.get();
                            } finally {
                                lock.writeLock().unlock();
                            }
                        }
                    },
                    logger);
        }
        
        public LockingCachedFunction<ZNodePath,AssignedVolumeBranches> lookup() {
            return lookup;
        }

        /**
         * Assumes write lock is held.
         */
        public boolean add(VolumeVersion<?> volume) {
            if (volume instanceof AssignedVolumeBranches) {
                overlay.add(volume.getState().getVersion().longValue(), volume.getDescriptor().getPath(), volume.getDescriptor().getId());
                for (Map.Entry<ZNodeName, Identifier> leaf: ((RegionAndBranches) volume.getState().getValue()).getBranches().entrySet()) {
                    ZNodePath path = volume.getDescriptor().getPath().join(leaf.getKey());
                    overlay.add(volume.getState().getVersion().longValue(), path, leaf.getValue());
                }
                OptionalNode<Promise<AssignedVolumeBranches>> lookup = lookups.get(volume.getDescriptor().getPath());
                new PathLookupUpdater(((AssignedVolumeBranches) volume)).apply(lookup);
            } else {
                remove(volume.getDescriptor().getPath(), volume.getDescriptor().getId());
            }
            return true;
        }

        /**
         * Assumes write lock is held.
         */
        public boolean remove(ZNodePath path, Identifier id) {
            return overlay.remove(path, id);
        }

        /**
         * Assumes write lock is held.
         */
        public void clear() {
            for (OptionalNode<Promise<AssignedVolumeBranches>> node: lookups) {
                if (node.isPresent()) {
                    node.get().cancel(false);
                }
            }
            lookups.clear();
            overlay.clear();
        }
    }
    
    protected final class PathLookupListener extends SimpleToStringListenableFuture<AssignedVolumeBranches> implements Runnable {
        private final OptionalNode<Promise<AssignedVolumeBranches>> node;
        
        public PathLookupListener(OptionalNode<Promise<AssignedVolumeBranches>> node) {
            super(node.get());
            this.node = node;
        }

        @Override
        public void run() {
            if (isDone()) {
                lock.writeLock().lock();
                try {
                    if (node.isPresent() && node.get() == delegate()) {
                        node.clear();
                        OptionalNode<Promise<AssignedVolumeBranches>> node = this.node;
                        while (!node.isPresent() && node.isEmpty() && (node.parent().get() != null)) {
                            if (node.remove()) {
                                node = node.parent().get();
                            }
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }
    
    protected static final class PathLookupUpdater implements Function<OptionalNode<Promise<AssignedVolumeBranches>>, Void> {
        
        private final AssignedVolumeBranches volume;
        
        public PathLookupUpdater(AssignedVolumeBranches volume) {
            this.volume = volume;
        }

        /**
         * Assumes write lock is held.
         */
        @Override
        public Void apply(OptionalNode<Promise<AssignedVolumeBranches>> input) {
            if (input != null) {
                if (AssignedVolumeBranches.contains(volume, input.path())) {
                    for (OptionalNode<Promise<AssignedVolumeBranches>> child: input.values()) {
                        apply(child);
                    }
                    if (input.isPresent()) {
                        input.get().set(volume);
                    }
                }
            }
            return null;
        }
    }
}
