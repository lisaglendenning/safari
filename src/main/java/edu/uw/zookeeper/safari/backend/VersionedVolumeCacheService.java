package edu.uw.zookeeper.safari.backend;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.Automaton.Transition;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.AbstractWatchMatchListener;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Lease;
import edu.uw.zookeeper.safari.VersionTransition;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.VersionedValue;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.data.VolumeBranchListener;
import edu.uw.zookeeper.safari.data.VolumeDescriptorCache;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.volume.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.volume.AssignedVolumeLeaves;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;
import edu.uw.zookeeper.safari.volume.VolumeDescriptor;
import edu.uw.zookeeper.safari.volume.VolumeVersion;

public final class VersionedVolumeCacheService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public VersionedVolumeCacheService getVersionedVolumeCacheService(
                final @Region Identifier region,
                VolumeDescriptorCache volumes,
                ControlClientService control,
                ScheduledExecutorService scheduler,
                ServiceMonitor monitor) {
            VersionedVolumeCacheService instance = create(
                    Predicates.equalTo(region), 
                    volumes.asLookup(), control, scheduler);
            VersionVolumesWatcher.newInstance(instance, control);
            monitor.add(instance);
            return instance;
        }
    }
    
    public static VersionedVolumeCacheService create(
            Predicate<Identifier> isResident,
            CachedFunction<Identifier, ZNodePath> idToPath,
            ControlClientService control,
            ScheduledExecutorService scheduler) {
        return new VersionedVolumeCacheService(isResident, idToPath, control, scheduler, LogManager.getLogger(VersionedVolumeCacheService.class));
    }
    
    private final Logger logger;
    private final ScheduledExecutorService scheduler;
    private final ControlClientService control;
    private final Predicate<Identifier> isResident;
    private final CachedFunction<Identifier, ZNodePath> idToPath;
    private final List<WatchMatchListener> listeners;
    private final ConcurrentMap<Identifier, CachedVolume> volumes;
    private final Function<Identifier, CachedVolume.LeasedVersion> idToVersion;
    private final Function<Identifier, CachedFunction<Long, UnsignedLong>> idToZxid;
    private final Function<Identifier, CachedFunction<UnsignedLong, AssignedVolumeBranches>> idToVolume;
    
    protected VersionedVolumeCacheService(  
            Predicate<Identifier> isResident,
            CachedFunction<Identifier, ZNodePath> idToPath,
            ControlClientService control,
            ScheduledExecutorService scheduler,
            Logger logger) {
        this.logger = logger;
        this.scheduler = scheduler;
        this.control = control;
        this.isResident = isResident;
        this.idToPath = idToPath;
        this.listeners = ImmutableList.<WatchMatchListener>of(
                new VolumesDeletedListener(), 
                new VolumeDeletedListener(),
                new VersionUpdateListener(ControlSchema.Safari.Volumes.Volume.Log.Version.State.LABEL),
                new VersionUpdateListener(ControlSchema.Safari.Volumes.Volume.Log.Version.Lease.LABEL),
                new VersionUpdateListener(ControlSchema.Safari.Volumes.Volume.Log.Version.Xomega.LABEL));
        this.volumes = new MapMaker().makeMap();
        this.idToVersion = new Function<Identifier, CachedVolume.LeasedVersion>() {
            @Override
            public CachedVolume.LeasedVersion apply(final Identifier id) {
                return getCachedVolume(id).version();
            }
        };
        this.idToZxid = new Function<Identifier, CachedFunction<Long, UnsignedLong>>() {
            @Override
            public CachedFunction<Long, UnsignedLong> apply(final Identifier id) {
                return getCachedVolume(id).zxid().lookup();
            }
        };
        this.idToVolume = new Function<Identifier, CachedFunction<UnsignedLong, AssignedVolumeBranches>>() {
            @Override
            public CachedFunction<UnsignedLong, AssignedVolumeBranches> apply(final Identifier id) {
                return getCachedVolume(id).volume().lookup();
            }
        };
    }
    
    public CachedFunction<Identifier, ZNodePath> idToPath() {
        return idToPath;
    }
    
    public Function<Identifier, CachedVolume.LeasedVersion> idToVersion() {
        return idToVersion;
    }
    
    public Function<Identifier, CachedFunction<Long, UnsignedLong>> idToZxid() {
        return idToZxid;
    }
    
    public Function<Identifier, CachedFunction<UnsignedLong, AssignedVolumeBranches>> idToVolume() {
        return idToVolume;
    }
    
    @SuppressWarnings("unchecked")
    public void update(final ZNodePath versionPath) {
        control.materializer().cache().lock().readLock().lock();
        try {
            ControlSchema.Safari.Volumes.Volume.Log.Version versionNode = 
                    (ControlSchema.Safari.Volumes.Volume.Log.Version) control.materializer().cache().cache().get(versionPath);

            ControlSchema.Safari.Volumes.Volume.Log.Version.State stateNode = versionNode.state();
            // ignore unknown states
            if ((stateNode == null) || (stateNode.data().stamp() < 0L)) {
                return;
            }
            
            final UnsignedLong version = versionNode.name();
            final Optional<RegionAndLeaves> state = Optional.fromNullable(stateNode.data().get());
            final Identifier id = versionNode.log().volume().name();
            if (state.isPresent() && isResident.apply(state.get().getRegion())) {
                CachedVolume v = getCachedVolume(id);
                ControlSchema.Safari.Volumes.Volume.Log.Version.Lease leaseNode = versionNode.lease();
                // ignore unleased versions
                if (leaseNode == null) {
                    return;
                }
                
                if ((leaseNode.data().stamp() > 0L) && (leaseNode.data().get() != null)) {
                    if (leaseNode.stat().stamp() >= leaseNode.data().stamp()) {
                        Lease lease = Lease.valueOf(leaseNode.stat().get().getMtime(), leaseNode.data().get().longValue());
                        v.version().onSuccess(Pair.create(version, lease));
                    } else {
                        new RequestCallback(
                                versionPath, 
                                SubmittedRequests.submitRequests(
                                        control.materializer(),
                                        Operations.Requests.sync().setPath(leaseNode.path()).build(),
                                        Operations.Requests.getData().setPath(leaseNode.path()).build()));
                        return;
                    }
                }
                
                ControlSchema.Safari.Volumes.Volume.Path pathNode = versionNode.log().volume().getPath();
                CachedVolume.VersionToVolume.PathListener forPath = v.volume().listen(VersionedValue.valueOf(version, state.get()));
                if ((pathNode != null) && (pathNode.data().stamp() > 0L)) {
                    ZNodePath path = pathNode.data().get();
                    forPath.onSuccess(path);
                } else {
                    try {
                        ListenableFuture<ZNodePath> future = idToPath.apply(id);
                        Futures.addCallback(future, forPath, SameThreadExecutor.getInstance());
                    } catch (Exception e) {
                        forPath.onFailure(e);
                    }
                }
                
                ControlSchema.Safari.Volumes.Volume.Log.Version.Xomega xomegaNode = versionNode.xomega();
                if ((xomegaNode != null) && (xomegaNode.data().stamp() > 0L)) {
                    v.zxid().onSuccess(Pair.create(versionNode.name(), xomegaNode.data().get()));
                }
            } else {
                CachedVolume v = volumes.get(id);
                if (v != null) {
                    v.onFailure(new NonResidentVolumeException(version));
                }
            }
        } finally {
            control.materializer().cache().lock().readLock().unlock();
        }
    }
    
    public boolean remove(Identifier id) {
        CachedVolume v = volumes.remove(id);
        if (v != null) {
            v.onFailure(new NonResidentVolumeException(null));
            return true;
        }
        return false;
    }

    public void clear() {
        Iterator<CachedVolume> itr = Iterators.consumingIterator(volumes.values().iterator());
        while (itr.hasNext()) {
            itr.next().onFailure(new NonResidentVolumeException(null));
        }
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

    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }
    
    protected CachedVolume getCachedVolume(Identifier id) {
        CachedVolume v = volumes.get(id);
        if (v == null) {
            v = new CachedVolume(id);
            CachedVolume existing = volumes.putIfAbsent(id, v);
            if (existing != null) {
                v = existing;
            }
        }
        return v;
    }
    
    protected final class VolumesDeletedListener extends AbstractWatchMatchListener {

        public VolumesDeletedListener() {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.PATH, 
                    Watcher.Event.EventType.NodeDeleted));
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
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

    protected final class VersionUpdateListener extends AbstractWatchMatchListener {

        public VersionUpdateListener(ZNodeLabel label) {
            super(WatchMatcher.exact(
                    ControlSchema.Safari.Volumes.Volume.Log.Version.PATH.join(label), 
                    Watcher.Event.EventType.NodeCreated, 
                    Watcher.Event.EventType.NodeDataChanged));
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            update(((AbsoluteZNodePath) event.getPath()).parent());
        }
        
        @Override
        public void handleAutomatonTransition(Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }
    }
    
    protected class RequestCallback implements Runnable {

        protected final ZNodePath path;
        protected final SubmittedRequests<?,?> requests;
        
        public RequestCallback(
                ZNodePath path,
                SubmittedRequests<?,?> requests) {
            this.path = path;
            this.requests = requests;
            requests.addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void run() {
            if (requests.isDone()) {
                List<? extends Operation.ProtocolResponse<?>> responses;
                try {
                    responses = requests.get();
                } catch (Exception e) {
                    // TODO
                    throw Throwables.propagate(e);
                }
                for (Operation.ProtocolResponse<?> response: responses) {
                    try {
                        Operations.unlessError(response.record());
                    } catch (KeeperException e) {
                        // TODO
                        throw Throwables.propagate(e);
                    }
                }
                control.materializer().cache().lock().readLock().lock();
                try {
                    update(path);
                } finally {
                    control.materializer().cache().lock().readLock().unlock();
                }
            }
        }
    }
    
    protected final class CachedVolume {
        
        private final ReentrantReadWriteLock lock;
        private final Identifier id;
        private final VersionToVolume volume;
        private final ZxidToVersion zxid;
        private final LeasedVersion version;
        
        public CachedVolume(Identifier id) {
            this.lock = new ReentrantReadWriteLock(true);
            this.id = id;
            this.volume = new VersionToVolume();
            this.zxid = new ZxidToVersion();
            this.version = new LeasedVersion();
        }
        
        public VersionToVolume volume() {
            return volume;
        }
        
        public LeasedVersion version() {
            return version;
        }
        
        public ZxidToVersion zxid() {
            return zxid;
        }

        public void onFailure(Throwable t) {
            lock.writeLock().lock();
            try {
                volume.onFailure(t);
                zxid.onFailure(t);
                version.onFailure(t);
            } finally {
                lock.writeLock().unlock();
            }
        }
        
        protected final class ZxidToVersion implements FutureCallback<Pair<UnsignedLong, Long>> {
            private final RangeMap<Long, Object> values;
            private final NavigableMap<Long, Lookup> lookups;
            private final CachedFunction<Long, UnsignedLong> lookup;
            
            public ZxidToVersion() {
                this.values = TreeRangeMap.create();
                this.lookups = Maps.newTreeMap();
                final Function<Long, UnsignedLong> cached = new Function<Long, UnsignedLong>() {
                    @Override
                    public UnsignedLong apply(final Long zxid) {
                        lock.readLock().lock();
                        try {
                            Map.Entry<Range<Long>, Object> entry = values.getEntry(zxid);
                            if ((entry != null) && entry.getKey().hasUpperBound() && (entry.getValue() instanceof UnsignedLong)) {
                                return (UnsignedLong) entry.getValue();
                            } else {
                                return null;
                            }
                        } finally {
                            lock.readLock().unlock();
                        }
                    }
                };
                final AsyncFunction<Long, UnsignedLong> async = new AsyncFunction<Long, UnsignedLong>() {
                    @Override
                    public ListenableFuture<UnsignedLong> apply(final Long zxid) {
                        lock.writeLock().lock();
                        try {
                            Map.Entry<Range<Long>, Object> entry = values.getEntry(zxid);
                            if (entry != null) {
                                if (entry.getKey().hasUpperBound() && (entry.getValue() instanceof UnsignedLong)) {
                                    return Futures.immediateFuture((UnsignedLong) entry.getValue());
                                } else if (entry.getValue() instanceof Throwable) {
                                    return Futures.immediateFailedFuture((Throwable) entry.getValue());
                                }
                            }
                            Lookup lookup = lookups.get(zxid);
                            if (lookup != null) {
                                return lookup;
                            }
                            lookup = new Lookup(zxid, SettableFuturePromise.<UnsignedLong>create());
                            lookups.put(zxid, lookup);
                            lookup.addListener(lookup, SameThreadExecutor.getInstance());
                            return lookup;
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                };
                this.lookup = CachedFunction.create(
                        cached, 
                        async,
                        logger);
            }
            
            public CachedFunction<Long, UnsignedLong> lookup() {
                return lookup;
            }

            @Override
            public void onSuccess(Pair<UnsignedLong, Long> zxid) {
                control.materializer().cache().lock().readLock().lock();
                try {
                    lock.writeLock().lock();
                    try {
                        Range<Long> range = null;
                        Map.Entry<Range<Long>, Object> entry = values.getEntry(zxid.second());
                        if (entry != null) {
                            if (entry.getKey().hasUpperBound()) {
                                assert (entry.getKey().lowerBoundType() == BoundType.CLOSED);
                                assert (entry.getKey().upperEndpoint().equals(zxid.second()));
                                assert (entry.getValue().equals(zxid.first()));
                                return;
                            } else {
                                // IMPORTANT if this volume is resident again after being non-resident,
                                // we're assuming that a long enough time has passed 
                                // during the non-residency that all messages corresponding
                                // to the earlier resident version have been processed,
                                // otherwise we would have to do more work to
                                // calculate a tighter lower endpoint
                                if (entry.getKey().hasLowerBound()) {
                                    assert (entry.getKey().lowerBoundType() == BoundType.OPEN);
                                    range = Range.openClosed(entry.getKey().lowerEndpoint(), zxid.second());
                                } else {
                                    range = Range.atMost(zxid.second());
                                }
                                values.remove(entry.getKey());
                            }
                        }
                        
                        // find the xomega of the previous version
                        if (range == null) {
                            Optional<Long> lower = lowerEndpoint(zxid.first());
                            range =  lower.isPresent() ? Range.openClosed(lower.get(), zxid.second()) : Range.atMost(zxid.second());
                        }
                        
                        // Update map now that we have our range
                        values.put(range, zxid.first());
                            
                        // Finally, update any dependent futures
                        NavigableMap<Long, Lookup> toUpdate = range.hasLowerBound() ? 
                                lookups.subMap(range.lowerEndpoint(), false, range.upperEndpoint(), true) :
                                    lookups.headMap(range.upperEndpoint(), true);
                        Iterator<Lookup> itr = Iterators.consumingIterator(toUpdate.values().iterator());
                        while (itr.hasNext()) {
                            itr.next().set(zxid.first());
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                } finally {
                    control.materializer().cache().lock().readLock().unlock();
                }
            }
    
            @Override
            public void onFailure(Throwable t) {
                control.materializer().cache().lock().readLock().lock();
                try {
                    lock.writeLock().lock();
                    try {
                        if (t instanceof NonResidentVolumeException) {
                            UnsignedLong version = ((NonResidentVolumeException) t).getVersion();
                            // assume that we already know the xomega of the preceding resident version
                            Long lower = lowerEndpoint(version).get();
                            Object value = values.get(lower);
                            if (value != null) {
                                assert (value instanceof Throwable);
                            } else {
                                Optional<Long> upper = Optional.absent();
                                if (values.span().contains(lower)) {
                                    for (Range<Long> k: values.asMapOfRanges().keySet()) {
                                        if (k.hasLowerBound() && (k.lowerEndpoint().longValue() > lower.longValue()) && (!upper.isPresent() || upper.get().longValue() > k.lowerEndpoint().longValue())) {
                                            upper = Optional.of(k.lowerEndpoint());
                                        }
                                    }
                                }
                                Range<Long> range = upper.isPresent() ? Range.openClosed(lower, upper.get()) : Range.atLeast(lower);
                                values.put(range, t);
                                
                                NavigableMap<Long, Lookup> toUpdate = lookups.subMap(range.lowerEndpoint(), false, range.upperEndpoint(), true);
                                Iterator<Lookup> itr = Iterators.consumingIterator(toUpdate.values().iterator());
                                while (itr.hasNext()) {
                                    itr.next().setException(t);
                                }
                            }
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                } finally {
                    control.materializer().cache().lock().readLock().unlock();
                }
            }
            
            // assumes cache lock is held
            protected Optional<Long> lowerEndpoint(final UnsignedLong version) {
                ControlSchema.Safari.Volumes.Volume.Log log = 
                        (ControlSchema.Safari.Volumes.Volume.Log) control.materializer().cache().cache().get(ControlSchema.Safari.Volumes.Volume.Log.pathOf(id));
                assert (log != null);
                ZNodeName key = ZNodeName.fromString(version.toString());
                Map.Entry<ZNodeName, ControlZNode<?>> prev;
                for (prev = log.lowerEntry(key); prev != null; key = prev.getKey()) {
                    ControlSchema.Safari.Volumes.Volume.Log.Version versionNode = (ControlSchema.Safari.Volumes.Volume.Log.Version) prev.getValue();
                    ControlSchema.Safari.Volumes.Volume.Log.Version.State stateNode = versionNode.state();
                    if ((stateNode == null) || (stateNode.data().stamp() < 0L)) {
                        throw new UnsupportedOperationException();
                    }
                    boolean resident = isResident.apply(stateNode.data().get().getRegion());
                    if (resident) {
                        ControlSchema.Safari.Volumes.Volume.Log.Version.Lease leaseNode = versionNode.lease();
                        if (leaseNode != null) {
                            ControlSchema.Safari.Volumes.Volume.Log.Version.Xomega xomegaNode = versionNode.xomega();
                            assert ((xomegaNode != null) && (xomegaNode.data().stamp() > 0L));
                            return Optional.of(xomegaNode.data().get());
                        }
                    }
                }
                return Optional.absent();
            }
            
            protected final class Lookup extends ForwardingPromise<UnsignedLong> implements Runnable {

                private final Long key;
                private final Promise<UnsignedLong> delegate;
                
                public Lookup(Long key, Promise<UnsignedLong> delegate) {
                    this.delegate = delegate;
                    this.key = key;
                }
                
                @Override
                public void run() {
                    if (! isDone()) {
                        return;
                    }
                    lock.writeLock().lock();
                    try {
                        Lookup v = lookups.remove(key);
                        if ((v != this) && !v.isDone()) {
                            try {
                                v.set(get());
                            } catch (ExecutionException e) {
                                v.setException(e.getCause());
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                }

                @Override
                protected Promise<UnsignedLong> delegate() {
                    return delegate;
                }
            }
        }
        
        protected final class VersionToVolume implements FutureCallback<VolumeVersion<?>> {
            
            private final Map<UnsignedLong, Object> values;
            private final Map<VersionedId, BranchesListener> listeners;
            private final Map<UnsignedLong, Lookup> lookups;
            private final CachedFunction<UnsignedLong, AssignedVolumeBranches> lookup;
            
            public VersionToVolume() {
                this.values = Maps.newHashMap();
                this.listeners = Maps.newHashMap();
                this.lookups = Maps.newHashMap();
                final Function<UnsignedLong, AssignedVolumeBranches> cached = new Function<UnsignedLong, AssignedVolumeBranches>() {
                    @Override
                    public AssignedVolumeBranches apply(final UnsignedLong version) {
                        lock.readLock().lock();
                        try {
                            Object value = values.get(version);
                            if (value instanceof AssignedVolumeBranches) {
                                return (AssignedVolumeBranches) value;
                            } else {
                                return null;
                            }
                        } finally {
                            lock.readLock().unlock();
                        }
                    }
                };
                final AsyncFunction<UnsignedLong, AssignedVolumeBranches> async = new AsyncFunction<UnsignedLong, AssignedVolumeBranches>() {
                    @Override
                    public ListenableFuture<AssignedVolumeBranches> apply(final UnsignedLong version) {
                        lock.writeLock().lock();
                        try {
                            Object value = values.get(version);
                            if (value != null) {
                                if (value instanceof AssignedVolumeBranches) {
                                    return Futures.immediateFuture((AssignedVolumeBranches) value);
                                } else {
                                    return Futures.immediateFailedFuture((Throwable) value);
                                }
                            }
                            Lookup lookup = lookups.get(version);
                            if (lookup != null) {
                                return lookup;
                            }
                            lookup = new Lookup(version, SettableFuturePromise.<AssignedVolumeBranches>create());
                            lookups.put(version, lookup);
                            lookup.addListener(lookup, SameThreadExecutor.getInstance());
                            return lookup;
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                };
                this.lookup = CachedFunction.create(
                        cached, 
                        async,
                        logger);
            }

            public CachedFunction<UnsignedLong, AssignedVolumeBranches> lookup() {
                return lookup;
            }

            @Override
            public void onSuccess(VolumeVersion<?> result) {
                lock.writeLock().lock();
                try {
                    if (result instanceof AssignedVolumeBranches) {
                        Object value = values.get(result.getState().getVersion());
                        if (value == null) {
                            AssignedVolumeBranches v = (AssignedVolumeBranches) result;
                            values.put(v.getState().getVersion(), v);
                            Lookup lookup = lookups.remove(v.getState().getVersion());
                            if (lookup != null) {
                                lookup.set(v);
                            }
                        } else {
                            assert (value.equals(result));
                        }
                        assert (! listeners.containsKey(VersionedId.valueOf(result.getState().getVersion(), result.getDescriptor().getId())));
                    } else {
                        AssignedVolumeLeaves v = (AssignedVolumeLeaves) result;
                        VersionedId k = VersionedId.valueOf(v.getState().getVersion(), v.getDescriptor().getId());
                        if (!listeners.containsKey(k)) {
                            try {
                                new BranchesListener(k, VolumeBranchListener.volumeFromCache(k, v.getState().getValue(), idToPath));
                            } catch (Exception e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                lock.writeLock().lock();
                try {
                    if (t instanceof NonResidentVolumeException) {
                        UnsignedLong version = ((NonResidentVolumeException) t).getVersion();
                        Lookup lookup = lookups.remove(version);
                        if (lookup != null) {
                            lookup.setException(t);
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
            
            public PathListener listen(VersionedValue<RegionAndLeaves> state) {
                return new PathListener(state);
            }
            
            protected final class Lookup extends ForwardingPromise<AssignedVolumeBranches> implements Runnable {

                private final UnsignedLong key;
                private final Promise<AssignedVolumeBranches> delegate;
                
                public Lookup(UnsignedLong key, Promise<AssignedVolumeBranches> delegate) {
                    this.delegate = delegate;
                    this.key = key;
                }
                
                @Override
                public void run() {
                    if (! isDone()) {
                        return;
                    }
                    lock.writeLock().lock();
                    try {
                        Lookup v = lookups.remove(key);
                        if ((v != this) && !v.isDone()) {
                            try {
                                v.set(get());
                            } catch (ExecutionException e) {
                                v.setException(e.getCause());
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                }

                @Override
                protected Promise<AssignedVolumeBranches> delegate() {
                    return delegate;
                }
            }
            
            protected final class PathListener implements FutureCallback<ZNodePath> {

                private final VersionedValue<RegionAndLeaves> task;
                
                public PathListener(VersionedValue<RegionAndLeaves> task) {
                    this.task = task;
                }
                
                @Override
                public void onSuccess(ZNodePath path) {
                    AssignedVolumeLeaves v = AssignedVolumeLeaves.valueOf(
                            VolumeDescriptor.valueOf(id, path), task);
                    VersionToVolume.this.onSuccess(v);
                }

                @Override
                public void onFailure(Throwable t) {
                    VersionToVolume.this.onFailure(t);
                }
            }
            
            protected final class BranchesListener extends ToStringListenableFuture<AssignedVolumeBranches> implements Runnable {

                private final VersionedId version;
                
                public BranchesListener(VersionedId version, 
                        ListenableFuture<AssignedVolumeBranches> future) {
                    super(future);
                    this.version = version;
                    listeners.put(version, this);
                    addListener(this, SameThreadExecutor.getInstance());
                }
                
                @Override
                public void run() {
                    if (isDone()) {
                        lock.writeLock().lock();
                        try {
                            listeners.remove(version);
                            AssignedVolumeBranches volume;
                            try {
                                volume = get();
                            } catch (CancellationException e) {
                                return;
                            } catch (Exception e) {
                                // TODO
                                throw Throwables.propagate(e);
                            }
                            onSuccess(volume);
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                }
            }
        }
    
        protected final class LeasedVersion implements Supplier<VersionTransition>, Runnable, FutureCallback<Pair<UnsignedLong, Lease>> {
    
            private Optional<Pair<UnsignedLong, Lease>> lease;
            private Optional<? extends ScheduledFuture<?>> expiration;
            private VersionTransition transition;
            
            public LeasedVersion() {
                this.lease = Optional.absent();
                this.expiration = Optional.absent();
                this.transition = VersionTransition.absent(SettableFuturePromise.<UnsignedLong>create());
            }
            
            public boolean cancel(boolean mayInterruptIfRunning) {
                lock.writeLock().lock();
                try {
                    boolean cancelled = false;
                    if (expiration.isPresent()) {
                        cancelled = expiration.get().cancel(mayInterruptIfRunning);
                    }
                    cancelled |= transition.getNext().cancel(mayInterruptIfRunning);
                    return cancelled;
                } finally {
                    lock.writeLock().unlock();
                }
            }
            
            public void expire() {
                lock.writeLock().lock();
                try {
                    transition = VersionTransition.absent(transition.getNext());
                } finally {
                    lock.writeLock().unlock();
                }
            }
    
            @Override
            public void onSuccess(Pair<UnsignedLong, Lease> lease) {
                lock.writeLock().lock();
                try {
                    if (!this.lease.isPresent() || (this.lease.get().first().longValue() < lease.first().longValue()) 
                            || ((this.lease.get().first().longValue() == lease.first().longValue()) 
                                    && (this.lease.get().second().getStart() < lease.second().getStart()))) {
                        this.lease = Optional.of(lease);
                        run();
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
    
            @Override
            public void onFailure(Throwable t) {
                lock.writeLock().lock();
                try {
                    if (transition.getCurrent() != null) {
                        expire();
                    }
                    if (!transition.getNext().isDone()) {
                        ((Promise<?>) transition.getNext()).setException(t);
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
    
            @Override
            public VersionTransition get() {
                lock.readLock().lock();
                try {
                    return transition;
                } finally {
                    lock.readLock().unlock();
                }
            }
    
            @Override
            public void run() {
                lock.writeLock().lock();
                try {
                    if (expiration.isPresent() && expiration.get().isCancelled()) {
                        return;
                    }
                    if (lease.isPresent()) {
                        long remaining = lease.get().second().getRemaining();
                        if (remaining > 0) {
                            if (!Objects.equal(lease.get().first(), transition.getCurrent().orNull())) {
                                VersionTransition prev = transition;
                                transition = VersionTransition.present(lease.get().first(), SettableFuturePromise.<UnsignedLong>create());
                                if (!prev.getNext().isDone()) {
                                    ((Promise<UnsignedLong>) prev.getNext()).set(transition.getCurrent().get());
                                }
                            }
                            if (!expiration.isPresent() || (expiration.get().isDone() && !expiration.get().isCancelled())) {
                                expiration = Optional.of(scheduler.schedule(this, remaining, lease.get().second().getUnit()));
                            }
                        } else if (Objects.equal(lease.get().first(), transition.getCurrent().orNull())) {
                            expire();
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }
}
