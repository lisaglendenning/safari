package edu.uw.zookeeper.safari.storage.volumes;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Lease;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.DirectoryWatcherService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class VolumeVersionCache extends ServiceListenersService {
    
    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}

        @Provides @Singleton @Volumes
        public Function<Identifier, ReentrantReadWriteLock> getVolumeLocks() {
            final ConcurrentMap<Identifier, ReentrantReadWriteLock> locks = new MapMaker().softValues().makeMap();
            return new Function<Identifier, ReentrantReadWriteLock>() {
                @Override
                public ReentrantReadWriteLock apply(Identifier input) {
                    ReentrantReadWriteLock lock = locks.get(input);
                    if (lock == null) {
                        lock = new ReentrantReadWriteLock();
                        ReentrantReadWriteLock existing = locks.putIfAbsent(input, lock);
                        if (existing != null) {
                            lock = existing;
                        }
                    }
                    return lock;
                }
            };
        }
        
        @Provides @Singleton
        public VolumeVersionCache getVolumeVersionCache(
                final DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
                final DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume> entries,
                final DirectoryWatcherService<StorageSchema.Safari.Volumes> directory,
                final SchemaClientService<StorageZNode<?>,?> client,
                final @Volumes Function<Identifier, ReentrantReadWriteLock> locks,
                final ServiceMonitor monitor) {
            VolumeVersionCache instance = listen(versions, entries, directory, client, locks, ImmutableList.<Service.Listener>of());
            monitor.add(instance);
            return instance;
        }
        
        @Provides
        public CachedVolumes getCachedVolumes(
                VolumeVersionCache cache) {
            return cache.volumes();
        }
        
        @Override
        protected void configure() {
        }

        @Override
        public Key<? extends Service> getKey() {
            return Key.get(VolumeVersionCache.class);
        }
    }
    
    public static VolumeVersionCache listen(
            final DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
            final DirectoryEntryListener<StorageZNode<?>,StorageSchema.Safari.Volumes.Volume> entries,
            final DirectoryWatcherService<StorageSchema.Safari.Volumes> directory,
            final SchemaClientService<StorageZNode<?>,?> client,
            final Function<Identifier, ReentrantReadWriteLock> locks,
            Iterable<? extends Service.Listener> listeners) {
        final CachedVolumes volumes = CachedVolumes.create(client.materializer().cache(), locks);
        final VolumeVersionCache instance = new VolumeVersionCache(volumes, listeners);
        @SuppressWarnings("unchecked")
        final Watchers.PathToQueryCallback<?,?> versionWatcher = Watchers.PathToQueryCallback.create(
                PathToQuery.forRequests(
                        client.materializer(), 
                        Operations.Requests.sync(), 
                        Operations.Requests.getData().setWatch(true)),
                        Watchers.MaybeErrorProcessor.maybeNoNode(), 
                        Watchers.StopServiceOnFailure.create(instance));
        Watchers.FutureCallbackServiceListener.listen(
                Watchers.EventToPathCallback.create(versionWatcher), 
                instance, 
                client.notifications(), 
                WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Lease.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDataChanged), 
                instance.logger());
        final RunCachedVolume runner = new RunCachedVolume(volumes, instance);
        VolumeVersionEntryCacheListener.listen(runner, client.materializer(), client.cacheEvents(), instance, instance.logger());
        XalphaCacheListener.listen(versionWatcher, runner, client.materializer().cache(), client.cacheEvents(), instance, instance.logger());
        versions.add(new VolumeVersionCacheListener(versionWatcher, volumes));
        VolumeLogChildrenWatcher<?,?> logWatcher = VolumeLogChildrenWatcher.defaults(
                volumes, 
                client.materializer(), 
                directory);
        VolumeLogChildrenWatcher.listen(
                logWatcher, directory, client.notifications());
        entries.add(new VolumeCacheListener(logWatcher, volumes));
        return instance;
    }

    private final CachedVolumes volumes;
    
    protected VolumeVersionCache(
            CachedVolumes volumes,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.volumes = volumes;
    }
    
    public CachedVolumes volumes() {
        return volumes;
    }
    
    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }
    
    public static abstract class Version {
        
        private final UnsignedLong version;
        
        protected Version(
                UnsignedLong version) {
            assert (version != null);
            this.version = version;
        }
        
        public UnsignedLong getVersion() {
            return version;
        }
        
        @Override
        public int hashCode() {
            return getVersion().hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public String toString() {
            return toString(Objects.toStringHelper(this)).toString();
        }
        
        public Objects.ToStringHelper toString(Objects.ToStringHelper helper) {
            return helper.addValue(getVersion());
        }
    }

    public static final class LeasedVersion extends Version {
        
        private final Lease lease;
        private final Long xalpha;
        
        protected LeasedVersion(
                Lease lease,
                Long xalpha,
                UnsignedLong version) {
            super(version);
            assert (xalpha != null);
            assert (lease != null);
            this.xalpha = xalpha;
            this.lease = lease;
        }
        
        public Lease getLease() {
            return lease;
        }
        
        public Long getXalpha() {
            return xalpha;
        }
        
        public LeasedVersion renew(Lease lease) {
            return new LeasedVersion(lease, getXalpha(), getVersion());
        }
        
        public PastVersion terminate(Long xomega) {
            return new PastVersion(getXalpha(), xomega, getVersion());
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LeasedVersion)) {
                return false;
            }
            LeasedVersion other = (LeasedVersion) obj;
            if (!getVersion().equals(other.getVersion())) {
                return false;
            }
            assert (getXalpha().equals(other.getXalpha()));
            return getLease().equals(other.getLease());
        }
        
        @Override
        public Objects.ToStringHelper toString(Objects.ToStringHelper helper) {
            return super.toString(helper.addValue(getXalpha()).addValue(getLease()));
        }
    }
    
    public static final class PastVersion extends Version {
        
        private final Range<Long> zxids;

        protected PastVersion(
                Long xalpha,
                Long xomega,
                UnsignedLong version) {
            this(Range.closed(xalpha, xomega), version);
        }
        
        protected PastVersion(
                Range<Long> zxids,
                UnsignedLong version) {
            super(version);
            assert(zxids.lowerEndpoint().longValue() < zxids.upperEndpoint().longValue());
            this.zxids = zxids;
        }
        
        public Range<Long> getZxids() {
            return zxids;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PastVersion)) {
                return false;
            }
            PastVersion other = (PastVersion) obj;
            if (!getVersion().equals(other.getVersion())) {
                return false;
            }
            assert (getZxids().equals(other.getZxids()));
            return true;
        }
        
        @Override
        public Objects.ToStringHelper toString(Objects.ToStringHelper helper) {
            return super.toString(helper.addValue(getZxids()));
        }
    }
    
    public static final class RecentHistory extends AbstractPair<List<PastVersion>, Optional<LeasedVersion>> {

        protected RecentHistory(List<PastVersion> past,
                Optional<LeasedVersion> leased) {
            super(past, leased);
        }
        
        public List<PastVersion> getPast() {
            return first;
        }
        
        public boolean hasLeased() {
            return second.isPresent();
        }
        
        public LeasedVersion getLeased() {
            return second.get();
        }
    }
    
    /**
     * Note that volume write lock is held when futures are set.
     * 
     * Tries to avoid deadlock by acquiring cache read lock before volume write lock.
     */
    public static final class CachedVolume implements Runnable {
        
        private final ReentrantReadWriteLock lock;
        private final Identifier id;
        private final Deque<Version> versions;
        private final Set<Pending<?>> pending;
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final AbsoluteZNodePath path;
        private Optional<? extends Version> latest;
        private Promise<Version> future;
        
        protected CachedVolume(
                Identifier id,
                ReentrantReadWriteLock lock,
                LockableZNodeCache<StorageZNode<?>,?,?> cache) {
            this.id = id;
            this.lock = lock;
            this.cache = cache;
            this.path = StorageSchema.Safari.Volumes.Volume.pathOf(id);
            this.versions = Lists.newLinkedList();
            this.pending = Sets.newHashSet();
            this.future = SettableFuturePromise.<Version>create();
            this.latest = Optional.absent();
        }
        
        public Identifier getId() {
            return id;
        }
        
        public ReentrantReadWriteLock getLock() {
            return lock;
        }
    
        /**
         * assumes read lock is held
         */
        public Optional<? extends Version> getLatest() {
            return latest;
        }
        
        /**
         * assumes read lock is held
         */
        public ListenableFuture<Version> getFuture() {
            return future;
        }
    
        /**
         * assumes read lock is held
         * assumes version is likely to be close to latest
         */
        public Version get(UnsignedLong version) {
            Iterator<Version> itr = versions.descendingIterator();
            while (itr.hasNext()) {
                Version next = itr.next();
                int cmp = next.getVersion().compareTo(version);
                if (cmp == 0) {
                    return next;
                } else if (cmp < 0) {
                    break;
                }
            }
            return null;
        }
    
       /**
        * assumes read lock is held
        * assumes from is likely to be close to latest
        */
        public RecentHistory getRecentHistory(final UnsignedLong from) {
            LinkedList<PastVersion> past = Lists.newLinkedList();
            Optional<LeasedVersion> leased = Optional.absent();
            Iterator<Version> itr = versions.descendingIterator();
            while (itr.hasNext()) {
                Version next = itr.next();
                if (next.getVersion().longValue() < from.longValue()) {
                    break;
                } else {
                    if (next instanceof LeasedVersion) {
                        assert (!leased.isPresent());
                        leased = Optional.of((LeasedVersion) next);
                    } else {
                        past.addFirst((PastVersion) next);
                    }
                }
            }
            return new RecentHistory(past, leased);
        }

        @Override
        public void run() {
            // lock ordering is important
            cache.lock().readLock().lock();
            try {
                lock.writeLock().lock();
                try {
                    final StorageSchema.Safari.Volumes.Volume node = (StorageSchema.Safari.Volumes.Volume) cache.cache().get(path);
                    if (node != null) {
                        if (pending.isEmpty()) {
                            final StorageSchema.Safari.Volumes.Volume.Log log = node.getLog();
                            if (log != null) {
                                removeVersions(log);
                                addVersions(updateLastVersion(log), log);
                                updateLatest(log);
                            }    
                        } else if (latest.isPresent()) {
                            latest = Optional.absent();
                        }
                    } else {
                        versions.clear();
                        latest = Optional.absent();
                        nextFuture().cancel(false);
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        protected <V> void pending(ListenableFuture<V> future) {
            lock.writeLock().lock();
            try {
                new Pending<V>(future).run();
            } finally {
                lock.writeLock().unlock();   
            }
        }

        /**
         * assumes cache read lock and volume write lock is held
         */
        protected boolean removeVersions(
                final StorageSchema.Safari.Volumes.Volume.Log log) {
            Version prev = versions.peekFirst();
            while (!versions.isEmpty() && (log.isEmpty() || log.versions().firstEntry().getValue().name().longValue() > versions.peekFirst().getVersion().longValue())) {
                versions.removeFirst();
            }
            return !Objects.equal(prev, versions.peekFirst());
        }
        
        /**
         * assumes cache read lock and volume write lock is held
         */
        protected Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> updateLastVersion(
                final StorageSchema.Safari.Volumes.Volume.Log log) {
            Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> next = null;
            if (versions.isEmpty()) {
                next = log.versions().firstEntry();
            } else {
                final StorageSchema.Safari.Volumes.Volume.Log.Version version = log.version(versions.getLast().getVersion());
                if (versions.getLast() instanceof LeasedVersion) {
                    LeasedVersion last = (LeasedVersion) versions.getLast();
                    if (version.xomega() != null) {
                        final StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega xomega = version.xomega();
                        if (xomega.data().get() != null) {
                            PastVersion updated = last.terminate(xomega.data().get());
                            versions.removeLast();
                            versions.addLast(updated);
                            next = log.versions().higherEntry(version.parent().name());
                        }
                    } else if (version.lease() != null) {
                        final StorageSchema.Safari.Volumes.Volume.Log.Version.Lease lease = version.lease();
                        if (lease.stat().stamp() == lease.data().stamp()) {
                            if (lease.stat().get().getMtime() > last.getLease().getStart()) {
                                LeasedVersion updated = last.renew(Lease.valueOf(lease.stat().get().getMtime(), lease.data().get().longValue()));
                                versions.removeLast();
                                versions.addLast(updated);
                            } else {
                                assert (lease.stat().get().getMtime() == last.getLease().getStart());
                            }
                        }
                    }
                } else {
                    next = log.versions().higherEntry(version.parent().name());
                }
            }
            return next;
        }
        
        /**
         * assumes cache read lock and volume write lock is held
         */
        protected boolean addVersions(
                Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> next, 
                final StorageSchema.Safari.Volumes.Volume.Log log) {
            Version prev = versions.peekLast();
            while ((next != null) && (next.getValue().xalpha() != null)) {
                final Long xalpha = next.getValue().xalpha().data().get();
                if (xalpha != null) {
                    if (next.getValue().xomega() != null) {
                        final Long xomega = next.getValue().xomega().data().get();
                        if (xomega != null) {
                            versions.addLast(new PastVersion(xalpha, xomega, next.getValue().name()));
                            next = log.versions().higherEntry(next.getKey());
                            continue;
                        }
                    } else if (next.getValue().lease() != null) {
                        final StorageSchema.Safari.Volumes.Volume.Log.Version.Lease lease = next.getValue().lease();
                        if (lease.stat().stamp() == lease.data().stamp()) {
                           final UnsignedLong value = lease.data().get();
                           if (value != null) {
                               versions.addLast(new LeasedVersion(Lease.valueOf(lease.stat().get().getMtime(), value.longValue()), xalpha, next.getValue().name()));
                           }
                        }
                    }
                }
                break;
            }
            return !Objects.equal(prev, versions.peekLast());
        }

        /**
         * assumes cache read lock and volume write lock is held
         */
        protected boolean updateLatest(final StorageSchema.Safari.Volumes.Volume.Log log) {
            Optional<? extends Version> prev = latest;
            Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> highest = log.versions().lastEntry();
            if (highest != null) {
                if (versions.isEmpty()) {
                    if (latest.isPresent()) {
                        latest = Optional.absent();
                    }
                } else {
                    Version last = versions.getLast();
                    if (last.getVersion().equals(highest.getValue().name())) {
                        if (last != latest.orNull()) {
                            latest = Optional.of(last);
                            nextFuture().set(last);
                        }
                    } else {
                        assert (last.getVersion().longValue() < highest.getValue().name().longValue());
                        if (latest.isPresent()) {
                            latest = Optional.absent();
                        }
                    }
                }
            } else if (latest.isPresent()) {
                latest = Optional.absent();
            }
            return (prev != latest);
        }
        
        /**
         * assumes write lock is held
         */
        protected Promise<Version> nextFuture() {
            final Promise<Version> future = this.future;
            this.future = SettableFuturePromise.<Version>create();
            return future;
        }
        
        private final class Pending<V> extends SimpleToStringListenableFuture<V> implements Runnable {

            private Pending(ListenableFuture<V> delegate) {
                super(delegate);
                pending.add(this);
            }

            @Override
            public void run() {
                if (isDone()) {
                    lock.writeLock().lock();
                    try {
                        pending.remove(this);
                    } finally {
                        lock.writeLock().unlock();
                    }
                    CachedVolume.this.run();
                } else {
                    addListener(this, SameThreadExecutor.getInstance());
                }
            }
        }
    }

    public static final class CachedVolumes implements Function<Identifier, CachedVolume> {

        public static CachedVolumes create(
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Function<Identifier, ReentrantReadWriteLock> locks) {
            return create(
                    new MapMaker().<Identifier, CachedVolume>makeMap(),
                    cache,
                    locks);
        }
        
        public static CachedVolumes create(
                ConcurrentMap<Identifier, CachedVolume> volumes,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Function<Identifier, ReentrantReadWriteLock> locks) {
            return new CachedVolumes(
                    volumes, 
                    cache,
                    locks);
        }
        
        private final ConcurrentMap<Identifier, CachedVolume> volumes;
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final Function<Identifier, ReentrantReadWriteLock> locks;
        
        protected CachedVolumes(
                ConcurrentMap<Identifier, CachedVolume> volumes,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Function<Identifier, ReentrantReadWriteLock> locks) {
            this.volumes = volumes;
            this.cache = cache;
            this.locks = locks;
        }
        
        public ConcurrentMap<Identifier, CachedVolume> volumes() {
            return volumes;
        }
        
        public Function<Identifier, ReentrantReadWriteLock> locks() {
            return locks;
        }
        
        public LockableZNodeCache<StorageZNode<?>,?,?> cache() {
            return cache;
        }
        
        /**
         * Assumes that only existing volumes are asked for
         */
        @Override
        public CachedVolume apply(
                final Identifier id) {
            final ReentrantReadWriteLock lock = locks.apply(id);
            lock.writeLock().lock();
            try {
                CachedVolume volume = volumes.get(id);
                if (volume == null) {
                    volume = new CachedVolume(id, lock, cache);
                    CachedVolume existing = volumes.putIfAbsent(id, volume);
                    assert (existing == null);
                }
                return volume;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    protected static abstract class CachedVolumesListener extends LoggingWatchMatchListener {

        private final CachedVolumes volumes;
        
        protected CachedVolumesListener(
                CachedVolumes volumes,
                WatchMatcher match, 
                Logger logger) {
            super(match, logger);
            this.volumes = volumes;
        }
        
        protected CachedVolumesListener(
                CachedVolumes volumes,
                WatchMatcher match) {
            super(match);
            this.volumes = volumes;
        }
        
        public CachedVolumes volumes() {
            return volumes;
        }
    }
    
    protected static final class RunCachedVolume extends Watchers.StopServiceOnFailure<StorageSchema.Safari.Volumes.Volume> {

        private final CachedVolumes volumes;

        protected RunCachedVolume(
                CachedVolumes volumes,
                Service service) {
            super(service);
            this.volumes = volumes;
        }

        @Override
        public void onSuccess(
                StorageSchema.Safari.Volumes.Volume result) {
            CachedVolume volume = volumes.volumes().get(result.name());
            if (volume != null) {
                volume.run();
            }
        }
    }
    
    protected static final class XalphaCacheListener implements FutureCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha> {

        public static Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                FutureCallback<ZNodePath> query,
                FutureCallback<StorageSchema.Safari.Volumes.Volume> volume,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.listen(
                    cache, 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                            Watchers.EventToPathCallback.create(
                                    Watchers.PathToNodeCallback.create(
                                            new XalphaCacheListener(
                                                    query, volume), 
                                            cache.cache())), 
                            WatchMatcher.exact(
                                    StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha.PATH,
                                    Watcher.Event.EventType.NodeDataChanged), 
                            logger), 
                    logger);
        }
        
        private final FutureCallback<ZNodePath> query;
        private final FutureCallback<StorageSchema.Safari.Volumes.Volume> volume;
        
        protected XalphaCacheListener(
                FutureCallback<ZNodePath> query,
                FutureCallback<StorageSchema.Safari.Volumes.Volume> volume) {
            this.query = query;
            this.volume = volume;
        }
        
        @Override
        public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha result) {
            final StorageSchema.Safari.Volumes.Volume.Log.Version version = result.version();
            this.volume.onSuccess(version.log().volume());
            if (version.lease() == null) {
                query.onSuccess(version.path().join(StorageSchema.Safari.Volumes.Volume.Log.Version.Lease.LABEL));
            }
            if (version.xomega() == null) {
                query.onSuccess(version.path().join(StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.LABEL));
            }
        }

        @Override
        public void onFailure(Throwable t) {
            volume.onFailure(t);
        }
    }
    
    protected static final class VolumeVersionEntryCacheListener<T extends StorageZNode<?>> implements FutureCallback<T> {

        protected static List<? extends Watchers.CacheNodeCreatedListener<StorageZNode<?>>> listen(
                FutureCallback<StorageSchema.Safari.Volumes.Volume> volume,
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            ImmutableList.Builder<Watchers.CacheNodeCreatedListener<StorageZNode<?>>> listeners = ImmutableList.builder();
            for (Class<? extends StorageZNode<?>> type: ImmutableList.of(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Lease.class,
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.class)) {
                listeners.add(listen(type, volume, materializer, cacheEvents, service, logger));
            }
            return listeners.build();
        }
        
        protected static <T extends StorageZNode<?>> Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                Class<T> type,
                FutureCallback<StorageSchema.Safari.Volumes.Volume> volume,
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.listen(
                    materializer.cache(), 
                    service, 
                    cacheEvents, 
                    Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        new VolumeVersionEntryCacheListener<T>(
                                                volume), 
                                materializer.cache().cache())), 
                        WatchMatcher.exact(
                                materializer.schema().apply(type).path(), 
                                Watcher.Event.EventType.NodeCreated, 
                                Watcher.Event.EventType.NodeDataChanged),
                                logger),
                    logger);
        }

        private final FutureCallback<StorageSchema.Safari.Volumes.Volume> volume;
        
        protected VolumeVersionEntryCacheListener(
                FutureCallback<StorageSchema.Safari.Volumes.Volume> volume) {
            this.volume = volume;
        }

        @Override
        public void onSuccess(T result) {
            final StorageSchema.Safari.Volumes.Volume.Log.Version node = (StorageSchema.Safari.Volumes.Volume.Log.Version) result.parent().get();
            volume.onSuccess(node.log().volume());
        }

        @Override
        public void onFailure(Throwable t) {
            volume.onFailure(t);
        }
    }
    
    protected static final class VolumeVersionCacheListener extends CachedVolumesListener {

        private final FutureCallback<ZNodePath> query;
        
        protected VolumeVersionCacheListener(
                FutureCallback<ZNodePath> query,
                CachedVolumes volumes) {
            super(volumes,
                    WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDeleted));
            this.query = query;
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            switch (event.getEventType()) {
            case NodeCreated:
            {
                final StorageSchema.Safari.Volumes.Volume.Log.Version node = (StorageSchema.Safari.Volumes.Volume.Log.Version) volumes().cache().cache().get(event.getPath());
                final StorageSchema.Safari.Volumes.Volume.Log log = node.log();
                ZNodeName lastKey = log.versions().lastKey();
                if ((node.parent().name() == lastKey) || (node.parent().name() == log.versions().lowerKey(lastKey))) {
                    if (node.xalpha() == null) {
                        query.onSuccess(event.getPath().join(StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha.LABEL));
                    }
                }
                break;
            }
            case NodeDeleted:
            {
                final StorageSchema.Safari.Volumes.Volume node = (StorageSchema.Safari.Volumes.Volume) volumes().cache().cache().get(((AbsoluteZNodePath) ((AbsoluteZNodePath) event.getPath()).parent()).parent());
                if (node != null) {
                    CachedVolume volume = volumes().volumes().get(node.name());
                    if (volume != null) {
                        volume.run();
                    }
                }
                break;
            }
            default:
                break;
            }
        }
    }

    protected static final class VolumeLogChildrenWatcher<O extends Operation.ProtocolResponse<?>,V> extends Watchers.PathToQueryCallback<O,V> {
    
        protected static Watchers.FutureCallbackServiceListener<?> listen(
                VolumeLogChildrenWatcher<?,?> callback,
                Service service,
                WatchListeners watch) {
            return Watchers.FutureCallbackServiceListener.listen(
                    Watchers.EventToPathCallback.create(callback), 
                    service, 
                    watch,
                    WatchMatcher.exact(
                            StorageSchema.Safari.Volumes.Volume.Log.PATH, 
                            Watcher.Event.EventType.NodeCreated, 
                            Watcher.Event.EventType.NodeChildrenChanged),
                    LogManager.getLogger(callback));
        }

        protected static <O extends Operation.ProtocolResponse<?>> VolumeLogChildrenWatcher<O,?> defaults(
                Function<Identifier, CachedVolume> volumes,
                ClientExecutor<? super Records.Request, O, ?> client,
                Service service) {
            return create(volumes, client, 
                    Watchers.MaybeErrorProcessor.maybeNoNode(), 
                    Watchers.StopServiceOnFailure.create(service));
        }

        protected static <O extends Operation.ProtocolResponse<?>,V> VolumeLogChildrenWatcher<O,V> create(
                Function<Identifier, CachedVolume> volumes,
                ClientExecutor<? super Records.Request, O, ?> client,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            return new VolumeLogChildrenWatcher<O,V>(volumes, client, processor, callback);
        }
        
        private final Function<Identifier, CachedVolume> volumes;
        
        @SuppressWarnings("unchecked")
        protected VolumeLogChildrenWatcher(
                Function<Identifier, CachedVolume> volumes,
                ClientExecutor<? super Records.Request, O, ?> client,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback) {
            super(PathToQuery.forRequests(
                        client, 
                        Operations.Requests.sync(), 
                        Operations.Requests.getChildren().setWatch(true)),
                    processor,
                    callback);
            this.volumes = volumes;
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            final Identifier id = Identifier.valueOf(((AbsoluteZNodePath) result).parent().label().toString());
            final CachedVolume volume = volumes.apply(id);
            Watchers.Query<O,V> future = Watchers.Query.call(
                    processor,
                    callback,
                    query.apply(result));
            volume.pending(future);
            future.run();
        }
    }

    protected static final class VolumeCacheListener extends CachedVolumesListener {
    
        private final FutureCallback<ZNodePath> query;
        
        protected VolumeCacheListener(
                FutureCallback<ZNodePath> query,
                CachedVolumes volumes) {
            super(volumes,
                    WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.PATH, 
                        Watcher.Event.EventType.NodeCreated,
                        Watcher.Event.EventType.NodeDeleted));
            this.query = query;
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            switch (event.getEventType()) {
            case NodeCreated:
            {
                final StorageSchema.Safari.Volumes.Volume node = (StorageSchema.Safari.Volumes.Volume) volumes().cache().cache().get(event.getPath());
                final Identifier id = node.name();
                volumes().apply(id);
                query.onSuccess(event.getPath().join(StorageSchema.Safari.Volumes.Volume.Log.LABEL));
                break;
            }
            case NodeDeleted:
            {
                final Identifier id = Identifier.valueOf(event.getPath().label().toString());
                CachedVolume volume = volumes().volumes().get(id);
                if (volume != null) {
                    volume.getLock().writeLock().lock();
                    try {
                        if (volumes().volumes().remove(id, volume)) {
                            volume.run();
                        }
                    } finally {
                        volume.getLock().writeLock().unlock();
                    }
                }
                break;
            }
            default:
                break;
            }
        }
    }
}
