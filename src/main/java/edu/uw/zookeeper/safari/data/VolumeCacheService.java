package edu.uw.zookeeper.safari.data;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Automaton.Transition;
import edu.uw.zookeeper.common.LockingCachedFunction;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.SharedLookup;
import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.AbstractWatchMatchListener;
import edu.uw.zookeeper.data.DefaultsNode;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlSchema;

public final class VolumeCacheService extends AbstractIdleService {

    public static Module module() {
        return Module.create();
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        protected Module() {}
        
        @Provides
        public VolumeDescriptorCache getVolumeDescriptorCache(
                VolumeCacheService volumes) {
            return volumes.idToPath();
        }
        
        @Provides @Singleton
        public VolumeCacheService getVolumeCacheService(
                ControlClientService control,
                ServiceMonitor monitor) {
            VolumeCacheService instance = VolumeCacheService.create(
                    VolumeDescriptorCache.create(), control);
            monitor.add(instance);
            LatestVolumesWatcher.create(instance, control);
            return instance;
        }

        @Override
        protected void configure() {
        }
    }
    
    public static VolumeCacheService create(
            VolumeDescriptorCache descriptors,
            ControlClientService control) {
        return new VolumeCacheService(descriptors, control);
    }

    private final Logger logger;
    private final ControlClientService control;
    private final VolumeDescriptorCache descriptors;
    private final ReentrantReadWriteLock lock;
    private final List<WatchMatchListener> listeners;
    private final VolumeById byId;
    private final VolumeByPath byPath;
    private final VolumeBranchCallbacks callbacks;
    
    protected VolumeCacheService(
            final VolumeDescriptorCache descriptors,
            final ControlClientService control) {
        this.logger = LogManager.getLogger(this);
        this.control = control;
        this.descriptors = descriptors;
        this.lock = new ReentrantReadWriteLock(true);
        this.listeners = ImmutableList.<WatchMatchListener>of(
                new VolumesDeletedListener(), 
                new VolumeDeletedListener(), 
                new VolumePathListener(), 
                new VolumeLatestListener(), 
                new VolumeStateListener());
        this.byId = new VolumeById();
        this.byPath = new VolumeByPath();
        this.callbacks = new VolumeBranchCallbacks();
    }
    
    public VolumeDescriptorCache idToPath() {
        return descriptors;
    }
    
    public LockingCachedFunction<Identifier, VersionedVolume> idToVolume() {
        return byId.lookup;
    }

    public AsyncFunction<ZNodePath, Volume> pathToVolume() {
        return byPath.lookup;
    }
    
    public Set<Volume> volumes() {
        lock.readLock().lock();
        try {
            ImmutableSet.Builder<Volume> volumes = ImmutableSet.builder();
            for (VersionedVolume volume: byId.cache.values()) {
                if (volume instanceof Volume) {
                    volumes.add((Volume) volume);
                }
            }
            return volumes.build();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public boolean add(VersionedVolume volume) {
        lock.writeLock().lock();
        try {
            logger.info("Adding volume {}", volume);
            descriptors.onSuccess(volume.getDescriptor());
            boolean added = byId.add(volume);
            if (added) {
                byPath.add(volume);
            }
            return added;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean remove(Identifier id) {
        lock.writeLock().lock();
        try {
            logger.info("Removing volume {}", id);
            boolean removed = byId.remove(id);
            ZNodePath path = descriptors.remove(id);
            if (path != null) {
                byPath.remove(path, id);
            }
            callbacks.remove(id);
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            descriptors.clear();
            byPath.clear();
            byId.clear();
            callbacks.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }

    @Override
    protected void startUp() {
        for (WatchMatchListener listener: listeners) {
            control.cacheEvents().subscribe(listener);
        }
    }

    @Override
    protected void shutDown() {
        for (WatchMatchListener listener: listeners) {
            control.cacheEvents().unsubscribe(listener);
        }
        clear();
    }
    
    protected final class VolumesDeletedListener extends AbstractWatchMatchListener {

        public VolumesDeletedListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.PATH, 
                    Watcher.Event.EventType.NodeDeleted));
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            logger.info("{}", event);
            clear();
        }
        
        @Override
        public void handleAutomatonTransition(Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }
    }
    
    protected final class VolumeDeletedListener extends AbstractWatchMatchListener {

        public VolumeDeletedListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.PATH, 
                    Watcher.Event.EventType.NodeDeleted));
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            Identifier id = Identifier.valueOf(event.getPath().label().toString());
            remove(id);
        }
        
        @Override
        public void handleAutomatonTransition(Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }
    }
    
    protected final class VolumePathListener extends AbstractWatchMatchListener {

        public VolumePathListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.Path.PATH, 
                    Watcher.Event.EventType.NodeCreated, Watcher.Event.EventType.NodeDataChanged));
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            ControlSchema.Safari.Volumes.Volume.Path node = 
                    (ControlSchema.Safari.Volumes.Volume.Path) control.materializer().cache().cache().get(event.getPath());
            if (node.data().stamp() > 0L) {
                ZNodePath path = node.data().get();
                assert (path != null);
                Identifier id = node.volume().name();
                logger.debug("Volume at {} is {}", path, id);
                descriptors.onSuccess(VolumeDescriptor.valueOf(id, path));
            }
        }
        
        @Override
        public void handleAutomatonTransition(Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }
    }
    
    protected final class VolumeLatestListener extends AbstractWatchMatchListener {

        public VolumeLatestListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH, 
                    Watcher.Event.EventType.NodeCreated, Watcher.Event.EventType.NodeDataChanged));
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            ControlSchema.Safari.Volumes.Volume.Log.Latest node = 
                    (ControlSchema.Safari.Volumes.Volume.Log.Latest) control.materializer().cache().cache().get(event.getPath());
            if (node.data().stamp() > 0L) {
                lock.writeLock().lock();
                try {
                    UnsignedLong version = node.data().get();
                    assert (version != null);
                    Identifier id = node.log().volume().name();
                    logger.info("Latest version of volume {} is {}", id, version);
                    VersionedVolume existing = byId.cache.get(id);
                    if ((existing == null) || (existing.getVersion().compareTo(version) < 0)) {
                        ControlSchema.Safari.Volumes.Volume.Log.Version versionNode = node.log().version(version);
                        if (versionNode != null) {
                            ControlSchema.Safari.Volumes.Volume.Log.Version.State stateNode = versionNode.state();
                            if ((stateNode != null) && (stateNode.stamp() > 0L)) {
                                callbacks.add(id, version, stateNode.data().get());
                            }
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
        
        @Override
        public void handleAutomatonTransition(Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }
    }
    
    protected final class VolumeStateListener extends AbstractWatchMatchListener {

        public VolumeStateListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.Log.Version.State.PATH, 
                    Watcher.Event.EventType.NodeCreated, Watcher.Event.EventType.NodeDataChanged));
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            ControlSchema.Safari.Volumes.Volume.Log.Version.State stateNode = 
                    (ControlSchema.Safari.Volumes.Volume.Log.Version.State) control.materializer().cache().cache().get(event.getPath());
            if (stateNode.data().stamp() > 0L) {
                lock.writeLock().lock();
                try {
                    UnsignedLong version = stateNode.version().name();
                    Identifier id = stateNode.version().log().volume().name();
                    ControlSchema.Safari.Volumes.Volume.Log.Latest latestNode = 
                            stateNode.version().log().latest();
                    if ((latestNode != null) && (latestNode.data().stamp() > 0L) && latestNode.data().get().equals(version)) {
                        VersionedVolume existing = byId.cache.get(id);
                        if ((existing == null) || (existing.getVersion().compareTo(version) < 0)) {
                            callbacks.add(id, version, stateNode.data().get());
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
        
        @Override
        public void handleAutomatonTransition(Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }
    }
    
    protected final class VolumeByPath {

        private final VolumeOverlay overlay;
        private final NameTrie<PathLookup> lookups;
        private final LockingCachedFunction<ZNodePath, Volume> lookup;
        
        public VolumeByPath() {
            this.overlay = VolumeOverlay.empty();
            this.lookups = SimpleLabelTrie.forRoot(new PathLookup(AbstractNameTrie.<PathLookup>rootPointer()));
            this.lookup = LockingCachedFunction.create(
                    lock.writeLock(),
                    new Function<ZNodePath, Volume>() {
                        @Override
                        public @Nullable
                        Volume apply(ZNodePath path) {
                            lock.readLock().lock();
                            try {
                                StampedValue<Identifier> id = overlay.apply(path);
                                if (id != null) {
                                    Volume volume = (Volume) byId.cache.get(id.get());
                                    if ((volume != null) && Volume.contains(volume, path)) {
                                        return volume;
                                    }
                                }
                                return null;
                            } finally {
                                lock.readLock().unlock();
                            }
                        }
                    }, 
                    new AsyncFunction<ZNodePath, Volume>() {
                        @Override
                        public PathLookup apply(ZNodePath path) {
                            lock.writeLock().lock();
                            try {
                                PathLookup node = PathLookup.putIfAbsent(lookups, path);
                                node.reset();
                                return node;
                            } finally {
                                lock.writeLock().unlock();
                            }
                        }
                    },
                    logger);
        }
        
        public boolean add(VersionedVolume volume) {
            if (volume instanceof Volume) {
                overlay.add(volume.getVersion().longValue(), volume.getDescriptor().getPath(), volume.getDescriptor().getId());
                for (Map.Entry<ZNodeName, Identifier> leaf: ((Volume) volume).getBranches().entrySet()) {
                    ZNodePath path = volume.getDescriptor().getPath().join(leaf.getKey());
                    overlay.add(volume.getVersion().longValue(), path, leaf.getValue());
                }
                PathLookup node = lookups.get(volume.getDescriptor().getPath());
                new PathUpdater(((Volume) volume)).apply(node);
            } else {
                remove(volume.getDescriptor().getPath(), volume.getDescriptor().getId());
            }
            return true;
        }
        
        public boolean remove(ZNodePath path, Identifier id) {
            return overlay.remove(path, id);
            // TODO lookups ??
        }
        
        public void clear() {
            overlay.clear();
            for (PathLookup node: lookups) {
                node.cancel(true);
            }
            lookups.clear();
        }
    }
    
    protected static final class PathUpdater implements Function<PathLookup, Void> {
        
        private final Volume volume;
        
        public PathUpdater(Volume volume) {
            this.volume = volume;
        }
        
        @Override
        public Void apply(PathLookup input) {
            if (input != null) {
                if (Volume.contains(volume, input.path())) {
                    for (PathLookup child: input.values()) {
                        apply(child);
                    }
                    input.set(volume);
                    if (input.isEmpty()) {
                        input.remove();
                    }
                }
            }
            return null;
        }
    }
    
    protected final class PathLookup extends
            DefaultsNode.AbstractDefaultsNode<PathLookup> implements
            Promise<Volume> {

        private Promise<Volume> promise;

        public PathLookup(
                NameTrie.Pointer<? extends PathLookup> parent) {
            super(parent);
            this.promise = null;
            reset();
        }

        public synchronized void reset() {
            if ((promise == null) || promise.isDone()) {
                promise = LoggingPromise.create(logger,
                        SettableFuturePromise.<Volume> create());
            }
        }

        @Override
        public void addListener(Runnable listener,
                Executor executor) {
            promise().addListener(listener, executor);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return promise().cancel(mayInterruptIfRunning);
        }

        @Override
        public Volume get() throws InterruptedException,
        ExecutionException {
            return promise().get();
        }

        @Override
        public Volume get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            return promise().get(timeout, unit);
        }

        @Override
        public boolean isCancelled() {
            return promise().isCancelled();
        }

        @Override
        public boolean isDone() {
            return promise().isDone();
        }

        @Override
        public boolean set(Volume value) {
            return promise().set(value);
        }

        @Override
        public boolean setException(Throwable throwable) {
            return promise().setException(throwable);
        }

        protected synchronized Promise<Volume> promise() {
            return promise;
        }

        @Override
        protected PathLookup newChild(ZNodeName name) {
            return new PathLookup(AbstractNameTrie.weakPointer(name, this));
        }
    }

    protected final class VolumeById {

        private final Map<Identifier, VersionedVolume> cache;
        private final LockingCachedFunction<Identifier, VersionedVolume> lookup;
        
        public VolumeById() {
            this.cache = Maps.newHashMap();
            this.lookup = LockingCachedFunction.create(
                    lock.writeLock(),
                    new Function<Identifier, VersionedVolume>() {
                        @Override
                        public @Nullable
                        VersionedVolume apply(Identifier id) {
                            lock.readLock().lock();
                            try {
                                return cache.get(id);
                            } finally {
                                lock.readLock().unlock();
                            }
                        }
                    }, 
                    SharedLookup.create(
                            new AsyncFunction<Identifier, VersionedVolume>() {
                                @Override
                                public VolumeUpdater apply(Identifier id) {
                                    return new VolumeUpdater(id, SettableFuturePromise.<VersionedVolume>create());
                                }
                            }),
                    logger);
        }
        
        public boolean add(VersionedVolume volume) {
            VersionedVolume prev = cache.get(volume.getDescriptor().getId());
            if (prev != null) {
                assert (prev.getDescriptor().equals(volume.getDescriptor()));
                if (prev.getVersion().compareTo(volume.getVersion()) >= 0) {
                    return false;
                }
            }
            cache.put(volume.getDescriptor().getId(), volume);
            return true;
        }
        
        public boolean remove(Identifier id) {
            ListenableFuture<?> future = ((SharedLookup<?,?>) lookup.async()).first().get(id);
            if (future != null) {
                future.cancel(true);
            }
            return (cache.remove(id) != null);
        }

        public void clear() {
            for (ListenableFuture<?> future: ((SharedLookup<?,?>) lookup.async()).first().values()) {
                future.cancel(true);
            }
            cache.clear();
        }
    }
    
    protected final class VolumeUpdater extends PromiseTask<Identifier, VersionedVolume> {

        public VolumeUpdater(
                Identifier volume,
                Promise<VersionedVolume> delegate) {
            super(volume, delegate);
        }
        
        @Override
        public boolean set(VersionedVolume volume) {
            lock.writeLock().lock();
            try {
                if (! add(volume)) {
                    volume = byId.cache.get(volume.getDescriptor().getId());
                }
                assert (volume != null);
                return super.set(volume);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
    
    protected final class VolumeBranchCallbacks {

        private final ConcurrentMap<Pair<Identifier, UnsignedLong>, VolumeBranchCallback> callbacks;
        
        public VolumeBranchCallbacks() {
            this.callbacks = new MapMaker().makeMap();
        }
        
        public void add(
                final Identifier id, 
                final UnsignedLong version, 
                final @Nullable VolumeState state) {
            Pair<Identifier, UnsignedLong> k = Pair.create(id, version);
            if (callbacks.containsKey(k)) {
                return;
            }
            
            VolumeBranchCallback callback;
            try {
                callback = new VolumeBranchCallback(VolumeBranchListener.fromCache(k, state, idToPath().asLookup()));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            callbacks.put(k, callback);
            callback.addListener(callback, SameThreadExecutor.getInstance());
        }

        public void remove(final Identifier id) {
            Iterator<Pair<Identifier, UnsignedLong>> keys = callbacks.keySet().iterator();
            while (keys.hasNext()) {
                Pair<Identifier, UnsignedLong> next = keys.next();
                if (next.first().equals(id)) {
                    VolumeBranchCallback callback = callbacks.get(next);
                    if (callback != null) {
                        callback.cancel(true);
                    }
                }
            }
        }
        
        public void clear() {
            for (VolumeBranchCallback callback: callbacks.values()) {
                callback.cancel(true);
            }
        }
        
        protected class VolumeBranchCallback extends ForwardingListenableFuture<VersionedVolume> implements Runnable {
            
            private final VolumeBranchListener delegate;
            
            public VolumeBranchCallback(VolumeBranchListener delegate) {
                this.delegate = delegate;
            }
            
            @Override
            public void run() {
                if (! isDone()) {
                    return;
                }
                callbacks.remove(delegate.task(), this);
                try {
                    VersionedVolume volume = get();
                    ((VolumeUpdater) byId.lookup.async().apply(delegate.task().first())).set(volume);
                } catch (CancellationException e) {
                    return;
                } catch (Exception e) {
                    // TODO
                    throw Throwables.propagate(e);
                }
            }

            @Override
            protected VolumeBranchListener delegate() {
                return delegate;
            }
        }
    }
}
