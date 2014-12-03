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
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;
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

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
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
import edu.uw.zookeeper.safari.VersionedValue;
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
                        Watchers.StopServiceOnFailure.create(instance, instance.logger()));
        Watchers.FutureCallbackServiceListener.listen(
                Watchers.EventToPathCallback.create(versionWatcher), 
                instance, 
                client.notifications(), 
                WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Lease.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDataChanged), 
                instance.logger());
        final RunCachedVolume runner = new RunCachedVolume(volumes, instance, instance.logger());
        VolumeVersionEntryCacheListener.listen(runner, client.materializer(), client.cacheEvents(), instance, instance.logger());
        XalphaCacheListener.listen(versionWatcher, runner, client.materializer().cache(), client.cacheEvents(), instance, instance.logger());
        versions.add(new VolumeVersionCacheListener(
                        client.materializer(), volumes));
        VolumeLogChildrenWatcher<?,?> logWatcher = VolumeLogChildrenWatcher.defaults(
                volumes, 
                client.materializer(), 
                directory,
                instance.logger());
        VolumeLogChildrenWatcher.listen(
                logWatcher, directory, client.notifications());
        entries.add(new VolumeCacheListener(logWatcher, volumes));
        VersionStateChangeListener.listen(
                volumes, 
                instance, 
                client.notifications(), 
                instance.logger());
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
        return MoreExecutors.directExecutor();
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
            return toString(MoreObjects.toStringHelper(this)).toString();
        }
        
        public MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
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
        public MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
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
        public MoreObjects.ToStringHelper toString(MoreObjects.ToStringHelper helper) {
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
    
    public static enum VersionState {
        UNKNOWN, XALPHA, XOMEGA;
    }
    
    /**
     * Note that volume write lock is held when futures are set.
     * 
     * Tries to avoid deadlock by acquiring cache read lock before volume write lock.
     */
    public static final class CachedVolume implements Runnable {
        
        private final Logger logger;
        private final ReentrantReadWriteLock lock;
        private final Identifier id;
        private final Deque<Version> versions;
        private final Set<Pending<?>> pending;
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final AbsoluteZNodePath path;
        // only remember most recent, but enough to rollback
        // if a version is aborted
        private final LinkedList<VersionedValue<VersionState>> states;
        private Optional<? extends Version> latest;
        private Promise<Pair<? extends Version, VersionedValue<VersionState>>> future;
        
        protected CachedVolume(
                Identifier id,
                ReentrantReadWriteLock lock,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Logger logger) {
            this.logger = logger;
            this.id = id;
            this.lock = lock;
            this.cache = cache;
            this.path = StorageSchema.Safari.Volumes.Volume.pathOf(id);
            this.versions = Lists.newLinkedList();
            this.states = Lists.newLinkedList();
            this.pending = Sets.newHashSet();
            this.future = null;
            this.latest = Optional.absent();
            
            nextFuture();
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
        public boolean hasPendingVersion() {
            return !pending.isEmpty();
        }
        
        /**
         * Present if there are no pending notifications for
         * new versions and we have seen the xalpha and either
         * the xomega or lease for the highest version.
         * 
         * assumes read lock is held
         */
        public Optional<? extends Version> getLatest() {
            return pending.isEmpty() ? latest : Optional.<Version>absent();
        }
        
        /**
         * Most recent ordered versions, high = first
         * assumes read lock is held
         */
        public Deque<VersionedValue<VersionState>> getVersionState() {
            return states;
        }
        
        /**
         * assumes read lock is held
         */
        public ListenableFuture<Pair<? extends Version, VersionedValue<VersionState>>> getFuture() {
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

        public boolean updateVersionState(VersionedValue<VersionState> state) {
            // lock ordering is important
            cache.lock().readLock().lock();
            try {
                lock.writeLock().lock();
                try {
                    logger.entry(this, state);
                    // tried to do this with a ListIterator but it was buggy?
                    boolean updated = false;
                    if (states.isEmpty() || (states.getFirst().getVersion().longValue() < state.getVersion().longValue())) {
                        states.addFirst(state);
                        updated = true;
                    } else {
                        int index = 0;
                        for (index = 0; index < states.size(); ++index) {
                            VersionedValue<VersionState> v = states.get(index);
                            if (v.getVersion().equals(state.getVersion())) {
                                if (v.getValue().compareTo(state.getValue()) < 0) {
                                    states.set(index, state);
                                    updated = true;
                                }
                                break;
                            } else {
                                assert (v.getVersion().longValue() > state.getVersion().longValue());
                            }
                        }
                    }
                    if (updated) {
                        if (VersionState.UNKNOWN.compareTo(state.getValue()) < 0) {
                            // garbage collect old versions
                            Iterator<VersionedValue<VersionState>> itr = states.iterator();
                            while (itr.hasNext()) {
                                if (itr.next().getVersion().longValue() < state.getVersion().longValue()) {
                                    itr.remove();
                                }
                            }
                        }
                        logger.trace("{}", this);
                    }
                    return logger.exit(updated);
                } finally {
                    lock.writeLock().unlock();
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }

        @Override
        public void run() {
            // lock ordering is important
            cache.lock().readLock().lock();
            try {
                lock.writeLock().lock();
                try {
                    logger.entry(this);
                    final StorageSchema.Safari.Volumes.Volume node = (StorageSchema.Safari.Volumes.Volume) cache.cache().get(path);
                    if (node != null) {
                        if (pending.isEmpty()) {
                            final StorageSchema.Safari.Volumes.Volume.Log log = node.getLog();
                            if (log != null) {
                                boolean updated = removeVersions(log);
                                updated = addVersions(updateLastVersion(log), log) || updated;
                                updated = updateLatest(log) || updated;
                                if (updated) {
                                    set();
                                }
                            }    
                        }
                    } else {
                        versions.clear();
                        latest = Optional.absent();
                        nextFuture().cancel(false);
                    }
                    logger.exit();
                } finally {
                    lock.writeLock().unlock();
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        @Override
        public String toString() {
            lock.readLock().lock();
            try {
                MoreObjects.ToStringHelper toString = MoreObjects.toStringHelper(this).add("id", id);
                if (pending.isEmpty()) {
                    toString.add("latest", latest).add("versions", versions).add("states", states);
                } else {
                    toString.add("pending", pending);
                }
                return toString.toString();
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * assumes cache read lock and volume write lock is held
         */
        protected boolean set() {
            return nextFuture().set(Pair.create(latest.orNull(), states.peekFirst()));
        }
        
        /**
         * assumes cache read lock is held
         */
        protected <V> void pending(ListenableFuture<V> future) {
            lock.writeLock().lock();
            try {
                LoggingFutureListener.listen(logger, new Pending<V>(future)).run();
            } finally {
                lock.writeLock().unlock();   
            }
        }

        /**
         * assumes cache read lock and volume write lock is held
         */
        protected boolean removeVersions(
                final StorageSchema.Safari.Volumes.Volume.Log log) {
            // prune garbage collected old versions
            Version prevLowVersion = versions.peekFirst();
            while (!versions.isEmpty() && (log.isEmpty() || (log.versions().firstEntry().getValue().name().longValue() > versions.getFirst().getVersion().longValue()))) {
                logger.debug("Version {} garbage collected for volume {}", versions.getFirst().getVersion(), id);
                versions.removeFirst();
            }
            boolean updated = prevLowVersion != versions.peekFirst();
            // prune aborted versions
            VersionedValue<VersionState> prevLastState = states.peekFirst();
            while (!states.isEmpty() && (log.isEmpty() || (states.getFirst().getVersion().longValue() > log.versions().lastEntry().getValue().name().longValue()))) {
                assert (states.getFirst().getValue() == VersionState.UNKNOWN);
                logger.debug("Version {} aborted for volume {}", states.getFirst().getVersion(), id);
                states.removeFirst();
            }
            updated = updated || prevLastState != states.peekLast();
            return updated;
        }
        
        /**
         * assumes cache read lock and volume write lock is held
         * 
         * @return entry greater or equal to the last Version
         */
        protected Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> updateLastVersion(
                final StorageSchema.Safari.Volumes.Volume.Log log) {
            Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> next;
            if (versions.isEmpty()) {
                // assume we only care about two highest
                next = log.versions().lastEntry();
                if ((next != null) && (next.getKey() != log.versions().firstKey()) && (next.getValue().xalpha() == null)) {
                    next = log.versions().lowerEntry(next.getKey());
                }
            } else {
                final StorageSchema.Safari.Volumes.Volume.Log.Version version = log.version(versions.getLast().getVersion());
                if (versions.getLast() instanceof LeasedVersion) {
                    LeasedVersion last = (LeasedVersion) versions.getLast();
                    Version updated = last;
                    final StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega xomega = version.xomega();
                    if (xomega != null) {
                        if (xomega.data().get() != null) {
                            updated = last.terminate(xomega.data().get());
                            next = log.versions().higherEntry(version.parent().name());
                        } else {
                            next = log.versions().ceilingEntry(version.parent().name());
                        }
                    } else {
                        final StorageSchema.Safari.Volumes.Volume.Log.Version.Lease lease = version.lease();
                        if (lease != null) {
                            if (lease.stat().stamp() == lease.data().stamp()) {
                                final long start = lease.stat().get().getMtime();
                                if (start > last.getLease().getStart()) {
                                    updated = last.renew(Lease.valueOf(start, lease.data().get().longValue()));
                                } else {
                                    assert (lease.stat().get().getMtime() == last.getLease().getStart());
                                }
                            }
                        }
                        next = log.versions().higherEntry(version.parent().name());
                    }
                    if (updated != last) {
                        logger.debug("Updated version {} to {} for volume {}", last, updated, id);
                        versions.removeLast();
                        versions.addLast(updated);
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
            boolean updated = false;
            Version prevHighVersion = versions.peekLast();
            while ((next != null) && (next.getValue().xalpha() != null)) {
                final StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega xomega = next.getValue().xomega();
                VersionState state = (xomega != null) ? VersionState.XOMEGA : VersionState.XALPHA;
                updated = updateVersionState(VersionedValue.valueOf(next.getValue().name(), state)) || updated;
                if (versions.isEmpty() || (versions.peekLast().getVersion().longValue() < next.getValue().name().longValue())) {
                    final Long xalpha = next.getValue().xalpha().data().get();
                    if (xalpha != null) {
                        if (xomega != null) {
                            if (xomega.data().get() != null) {
                                PastVersion version = new PastVersion(xalpha, xomega.data().get(), next.getValue().name());
                                logger.debug("Added {} for volume {}", version, id);
                                versions.addLast(version);
                                next = log.versions().higherEntry(next.getKey());
                                continue;
                            }
                        } else {
                            final StorageSchema.Safari.Volumes.Volume.Log.Version.Lease lease = next.getValue().lease();
                            if (lease != null) {
                                if (lease.stat().stamp() == lease.data().stamp()) {
                                   final UnsignedLong duration = lease.data().get();
                                   if (duration != null) {
                                       LeasedVersion version = new LeasedVersion(Lease.valueOf(lease.stat().get().getMtime(), duration.longValue()), xalpha, next.getValue().name());
                                       logger.debug("Added {} for volume {}", version, id);
                                       versions.addLast(version);
                                   }
                                }
                            }
                        }
                    }
                }
                next = log.versions().higherEntry(next.getKey());
                assert ((next == null) || (next.getValue().xalpha() == null));
            }
            while (next != null) {
                updated = updateVersionState(VersionedValue.valueOf(next.getValue().name(), VersionState.UNKNOWN)) || updated;
                next = log.versions().higherEntry(next.getKey());
                assert (next == null);
            }
            updated = updated || (prevHighVersion != versions.peekLast());
            return updated;
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
                            logger.debug("Latest version of {} is {}", id, last);
                            latest = Optional.of(last);
                        }
                    } else {
                        assert (last.getVersion().longValue() < highest.getValue().name().longValue());
                        if (latest.isPresent()) {
                            latest = Optional.absent();
                        }
                    }
                }
            } else {
                if (latest.isPresent()) {
                    latest = Optional.absent();
                }
            }
            return (prev != latest);
        }
        
        /**
         * assumes write lock is held
         */
        protected Promise<Pair<? extends Version, VersionedValue<VersionState>>> nextFuture() {
            final Promise<Pair<? extends Version, VersionedValue<VersionState>>> future = this.future;
            this.future = LoggingFutureListener.listen(logger, SettableFuturePromise.<Pair<? extends Version, VersionedValue<VersionState>>>create());
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
                    // lock ordering is important
                    cache.lock().readLock().lock();
                    try {
                        lock.writeLock().lock();
                        try {
                            if (pending.remove(this)) {
                                try {
                                    get();
                                    if (pending.isEmpty()) {
                                        ListenableFuture<?> future = CachedVolume.this.getFuture();
                                        CachedVolume.this.run();
                                        if (future == CachedVolume.this.getFuture()) {
                                            CachedVolume.this.set();
                                        }
                                    }
                                } catch (Exception e) {
                                    future.setException(e);
                                }
                            }
                        } finally {
                            lock.writeLock().unlock();
                        }
                    } finally {
                        cache.lock().readLock().unlock();
                    }
                } else {
                    addListener(this, MoreExecutors.directExecutor());
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
                    locks,
                    LogManager.getLogger(CachedVolumes.class));
        }
        
        public static CachedVolumes create(
                ConcurrentMap<Identifier, CachedVolume> volumes,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Function<Identifier, ReentrantReadWriteLock> locks,
                Logger logger) {
            return new CachedVolumes(
                    volumes, 
                    cache,
                    locks,
                    logger);
        }
        
        private final Logger logger;
        private final ConcurrentMap<Identifier, CachedVolume> volumes;
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final Function<Identifier, ReentrantReadWriteLock> locks;
        
        protected CachedVolumes(
                ConcurrentMap<Identifier, CachedVolume> volumes,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Function<Identifier, ReentrantReadWriteLock> locks,
                Logger logger) {
            this.logger = logger;
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
                    volume = new CachedVolume(id, lock, cache, logger);
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
    
    protected static final class RunCachedVolume extends Watchers.StopServiceOnFailure<StorageSchema.Safari.Volumes.Volume, Service> {

        private final CachedVolumes volumes;

        protected RunCachedVolume(
                CachedVolumes volumes,
                Service service,
                Logger logger) {
            super(service, logger);
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

        private final AsyncFunction<? super ZNodePath, ? extends List<? extends Operation.ProtocolResponse<?>>> query;
        
        protected VolumeVersionCacheListener(
                final ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>, ?> client,
                CachedVolumes volumes) {
            super(volumes,
                    WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDeleted));
            this.query = new AsyncFunction<ZNodePath, List<Operation.ProtocolResponse<?>>>() {
                @SuppressWarnings("unchecked")
                final PathToRequests requests = PathToRequests.forRequests(
                        Operations.Requests.sync(), 
                        Operations.Requests.getData().setWatch(true));
                @Override
                public ListenableFuture<List<Operation.ProtocolResponse<?>>> apply(
                        ZNodePath input) throws Exception {
                    List<Records.Request> requests = this.requests.apply(input.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha.LABEL));
                    ImmutableList.Builder<ListenableFuture<? extends Operation.ProtocolResponse<?>>> futures = ImmutableList.builder();
                    for (Records.Request request: requests) {
                        futures.add(client.submit(request));
                    }
                    return Futures.allAsList(futures.build());
                }
                
            };
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
                        CachedVolume volume = volumes().apply(log.volume().name());
                        volume.getLock().writeLock().lock();
                        try {
                            // consider a new version pending until we verify whether it has an xalpha
                            try {
                                volume.pending(query.apply(event.getPath()));
                            } catch (Exception e) {
                                volume.future.setException(e);
                            }
                        } finally {
                            volume.getLock().writeLock().unlock();
                        }
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
                WatchListeners cacheEvents) {
            return Watchers.FutureCallbackServiceListener.listen(
                    Watchers.EventToPathCallback.create(callback), 
                    service, 
                    cacheEvents,
                    WatchMatcher.exact(
                            StorageSchema.Safari.Volumes.Volume.Log.PATH, 
                            Watcher.Event.EventType.NodeCreated, 
                            Watcher.Event.EventType.NodeChildrenChanged),
                    LogManager.getLogger(callback));
        }

        protected static <O extends Operation.ProtocolResponse<?>> VolumeLogChildrenWatcher<O,?> defaults(
                Function<Identifier, CachedVolume> volumes,
                ClientExecutor<? super Records.Request, O, ?> client,
                Service service,
                Logger logger) {
            return create(volumes, client, 
                    Watchers.MaybeErrorProcessor.maybeNoNode(), 
                    Watchers.StopServiceOnFailure.create(service, logger));
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

    protected static final class VersionStateChangeListener extends LoggingWatchMatchListener {

        /**
         * Assumes xalpha and xomega are watched.
         */
        public static List<WatchMatchServiceListener> listen(
                Function<Identifier, CachedVolume> volumes,
                Service service,
                WatchListeners notifications,
                Logger logger) {
            ImmutableList.Builder<WatchMatchServiceListener> listeners = ImmutableList.builder();
            for (VersionState state: ImmutableList.of(VersionState.XALPHA, VersionState.XOMEGA)) {
                listeners.add(listen(state, volumes, service, notifications, logger));
            }
            return listeners.build();
        }
        
        public static WatchMatchServiceListener listen(
                VersionState state,
                Function<Identifier, CachedVolume> volumes,
                Service service,
                WatchListeners notifications,
                Logger logger) {
            ZNodePath path;
            switch (state) {
            case XALPHA:
                path = StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha.PATH;
                break;
            case XOMEGA:
                path = StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.PATH;
                break;
            default:
                throw new IllegalArgumentException(String.valueOf(state));
            }
            WatchMatchServiceListener listener = WatchMatchServiceListener.create(
                    service, 
                    notifications,
                    new VersionStateChangeListener(state, volumes, path, logger));
            listener.listen();
            return listener;
        }
        
        private final VersionState state;
        private final Function<Identifier, CachedVolume> volumes;
        
        protected VersionStateChangeListener(
                VersionState state,
                Function<Identifier, CachedVolume> volumes,
                ZNodePath path,
                Logger logger) {
            super(WatchMatcher.exact(path, Watcher.Event.EventType.NodeCreated), 
                    logger);
            this.state = state;
            this.volumes = volumes;
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            AbsoluteZNodePath path = (AbsoluteZNodePath) ((AbsoluteZNodePath) event.getPath()).parent();
            final UnsignedLong version = UnsignedLong.valueOf(path.label().toString());
            path = (AbsoluteZNodePath) ((AbsoluteZNodePath) path.parent()).parent();
            final Identifier id = Identifier.valueOf(path.label().toString());
            CachedVolume volume = volumes.apply(id);
            volume.updateVersionState(VersionedValue.valueOf(version, state));
        }
    }
}
