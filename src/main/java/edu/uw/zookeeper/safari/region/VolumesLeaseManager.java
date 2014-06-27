package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Lease;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.storage.StorageClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;

public class VolumesLeaseManager extends Watchers.CacheNodeCreatedListener<ControlZNode<?>> implements Function<ControlSchema.Safari.Volumes.Volume.Log.Version.State, Optional<VolumesLeaseManager.VolumeLeaseManager>> {

    public static VolumesLeaseManager defaults(
            final Identifier region,
            ScheduledExecutorService scheduler,
            Service service,
            ControlClientService control,
            StorageClientService storage,
            Configuration configuration) {
        final AsyncFunction<VersionedId, Boolean> voteOnRenewal = voteOnRenewal(service, control);
        final AsyncFunction<Identifier, Boolean> noSnapshot = noSnapshot(service, storage);
        return listen(
                Predicates.equalTo(region), 
                Functions.constant(
                        UnsignedLong.valueOf(
                                ConfigurableLeaseDuration.fromConfiguration(
                                        configuration)
                                    .value(TimeUnit.MILLISECONDS))),
                new AsyncFunction<VersionedId, Boolean>() {
                    @Override
                    public ListenableFuture<Boolean> apply(
                            final VersionedId volume)
                            throws Exception {
                        return Futures.transform(voteOnRenewal.apply(volume), 
                                new AsyncFunction<Boolean,Boolean>() {
                                    @Override
                                    public ListenableFuture<Boolean> apply(
                                            Boolean input) throws Exception {
                                        if (input.booleanValue()) {
                                            return noSnapshot.apply(volume.getValue());
                                        } else {
                                            return Futures.immediateFuture(input);
                                        }
                                    }
                        });
                    }
                },
                scheduler,
                service, 
                control);
    }
    
    public static AsyncFunction<VersionedId, Boolean> voteOnRenewal(
            final Service service,
            final ControlClientService control) {
        final Function<Optional<?>, Boolean> isAbsent = new Function<Optional<?>, Boolean>() {
            @Override
            public Boolean apply(Optional<?> input) {
                return !input.isPresent();
            }
        };
        final AsyncFunction<VersionedId, Boolean> isCommitted = new AsyncFunction<VersionedId, Boolean>() {
            @Override
            public ListenableFuture<Boolean> apply(
                    final VersionedId volume) throws Exception {
                return Futures.transform(
                        VolumeEntryAcceptor.defaults(
                            volume, 
                            service, 
                            control),
                        isAbsent,
                        SameThreadExecutor.getInstance());
            }
        };
        return isCommitted;
    }
    
    public static AsyncFunction<Identifier, Boolean> noSnapshot(
            final Service service,
            final StorageClientService storage) {
        return new AsyncFunction<Identifier, Boolean>() {
            @Override
            public ListenableFuture<Boolean> apply(Identifier volume)
                    throws Exception {
                final ZNodePath path = StorageSchema.Safari.Volumes.Volume.Snapshot.pathOf(volume);
                return Watchers.AbsenceWatcher.listen(
                        path,
                        new Supplier<Boolean>(){
                            @Override
                            public Boolean get() {
                                return Boolean.TRUE;
                            }
                }, 
                storage.materializer(), 
                service, 
                storage.notifications());
            }
        };
    }
    
    public static VolumesLeaseManager listen(
            Predicate<Identifier> isAssigned,
            Function<? super Identifier, UnsignedLong> durations,
            AsyncFunction<? super VersionedId, Boolean> doLease,
            ScheduledExecutorService scheduler,
            Service service,
            ControlClientService control) {
        VolumesLeaseManager listener = new VolumesLeaseManager(
                    isAssigned, 
                    durations,
                    doLease,
                    VolumesSchemaRequests.create(control.materializer()),
                    scheduler,
                    service, 
                    control.cacheEvents());
        listener.listen();
        return listener;
    }
    
    @Configurable(arg="lease", path="volume", key="lease", value="60 seconds", help="time")
    public static abstract class ConfigurableLeaseDuration {

        public static TimeValue fromConfiguration(Configuration configuration) {
            Configurable configurable = ConfigurableLeaseDuration.class.getAnnotation(Configurable.class);
            return TimeValue.fromString(
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key()));
        }
        
        protected ConfigurableLeaseDuration() {}
    }
    
    protected final ScheduledExecutorService scheduler;
    protected final VolumesSchemaRequests<?> requests;
    protected final Predicate<Identifier> isAssigned;
    protected final Function<? super Identifier, UnsignedLong> durations;
    protected final AsyncFunction<? super VersionedId, Boolean> doLease;
    protected final ConcurrentMap<VersionedId, VolumeLeaseManager> managers;
    
    protected VolumesLeaseManager(
            Predicate<Identifier> isAssigned,
            Function<? super Identifier, UnsignedLong> durations,
            AsyncFunction<? super VersionedId, Boolean> doLease,
            VolumesSchemaRequests<?> requests,
            ScheduledExecutorService scheduler,
            Service service,
            WatchListeners watch) {
        super(requests.getMaterializer().cache(), service, watch,
                WatchMatcher.exact(
                        ControlSchema.Safari.Volumes.Volume.Log.Latest.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDataChanged));
        this.scheduler = scheduler;
        this.requests = requests;
        this.isAssigned = isAssigned;
        this.durations = durations;
        this.doLease = doLease;
        this.managers = new MapMaker().makeMap();
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        ControlSchema.Safari.Volumes.Volume.Log.Latest node = (ControlSchema.Safari.Volumes.Volume.Log.Latest) cache.cache().get(event.getPath());
        if ((node != null) && (node.data().get() != null)) {
            final VersionedId volume = VersionedId.valueOf(node.data().get(), node.log().volume().name());
            if (! managers.containsKey(volume)) {
                ControlSchema.Safari.Volumes.Volume.Log.Version version = node.log().version(volume.getVersion());
                if ((version != null) && (version.state() != null) && (version.state().data().stamp() > 0L)) {
                    if ((version.state().data().get() != null) && isAssigned.apply(version.state().data().get().getRegion())) {
                        VolumeLeaseManager manager = new VolumeLeaseManager(
                                (VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests) requests.version(volume), 
                                durations.apply(volume.getValue()));
                        if (managers.putIfAbsent(volume, manager) == null) {
                            Services.listen(manager, service);
                        }
                    }
                } else {
                    Watchers.ReplayEvent.forListener(this, event, 
                            WatchMatcher.exact(
                                    version.path().join(ControlSchema.Safari.Volumes.Volume.Log.Version.State.LABEL), 
                                    Watcher.Event.EventType.NodeDataChanged)).listen();
                }
            }
        }
    }

    @Override
    public Optional<VolumeLeaseManager> apply(ControlSchema.Safari.Volumes.Volume.Log.Version.State input) {
        RegionAndLeaves state = input.data().get();
        if ((state != null) && isAssigned.apply(state.getRegion())) {
            final VersionedId volume = VersionedId.valueOf(input.version().name(), input.version().log().volume().name());
            return Optional.of(
                    new VolumeLeaseManager(
                    (VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests) requests.version(volume), 
                    durations.apply(volume.getValue())));
        }
        return Optional.absent();
    }
    
    public class VolumeLeaseManager extends Service.Listener implements Runnable {
        
        protected final Logger logger;
        protected final VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests version;
        protected final UnsignedLong duration;
        protected Optional<Lease> lease;
        protected Optional<? extends ListenableFuture<?>> future;
        
        public VolumeLeaseManager(
                VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests version,
                UnsignedLong duration) {
            this.logger = LogManager.getLogger(this);
            this.version = version;
            this.duration = duration;
            this.lease = Optional.absent();
            this.future = Optional.absent();
        }
        
        public AbsoluteZNodePath getPath() {
            return ControlSchema.Safari.Volumes.Volume.Log.Version.Lease.pathOf(version.volume().getVolume(), version.getVersion());
        }
        
        public VersionedId getVolume() {
            return VersionedId.valueOf(version.getVersion(), version.volume().getVolume());
        }
        
        public Optional<Lease> getCurrentLease() {
            requests.getMaterializer().cache().lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes.Volume.Log.Version.Lease node = (ControlSchema.Safari.Volumes.Volume.Log.Version.Lease) requests.getMaterializer().cache().cache().get(
                        getPath()); 
                if ((node != null) && (node.stat().get() != null) && (node.data().get() != null) && (node.stat().stamp() >= node.data().stamp())) { 
                    return Optional.of(Lease.valueOf(node.stat().get().getMtime(), node.data().get().longValue()));
                } else {
                    return Optional.absent();
                }
            } finally {
                requests.getMaterializer().cache().lock().readLock().unlock();
            }
        }
        
        public UnsignedLong getDuration() {
            return duration;
        }
        
        @Override
        public synchronized void run() {
            if (service.isRunning()) {
                Optional<? extends ListenableFuture<?>> next;
                try {
                    next = call();
                } catch (Exception e) {
                    // TODO
                    throw new UnsupportedOperationException(e);
                }
                if (next.isPresent()) {
                    future = next;
                    future.get().addListener(this, SameThreadExecutor.getInstance());
                }
            }
        }
        
        @Override
        public void running() {
            run();
        }

        @Override
        public void stopping(Service.State from) {
            super.stopping(from);
            managers.remove(getVolume(), this);
            if (future.isPresent()) {
                future.get().cancel(false);
            }
        }
        
        protected ListenableFuture<Boolean> doLease() {
            try {
                return doLease.apply(getVolume());
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
        }
        
        protected ListenableFuture<?> lease() {
            ImmutableList<? extends Records.Request> requests;
            if (lease.isPresent()) {
                requests = ImmutableList.of(version.lease().update(getDuration()));
            } else {
                requests = ImmutableList.<Records.Request>builder()
                        .add(version.lease().create(getDuration()))
                        .addAll(version.lease().get()).build();
            }
            return new LeaseRequestListener(requests);
        }
        
        protected synchronized Optional<Lease> updateLease() {
            lease = getCurrentLease();
            return lease;
        }
        
        protected Optional<? extends ListenableFuture<?>> call() throws Exception {
            if (future.isPresent()) {
                if (future.get().isDone()) {
                    if (future.get().isCancelled()) {
                        stopping(service.state());
                    } else {
                        Object result = future.get().get();
                        if (result instanceof Lease) {
                            final Lease lease = (Lease) result;
                            long remaining = lease.getRemaining();
                            if (remaining <= 0) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("LEASE EXPIRED for volume {}", getVolume());
                                }
                                return Optional.of(doLease());
                            } else {
                                scheduler.schedule(this, remaining, lease.getUnit());
                            }
                        } else {
                            if (((Boolean) result).booleanValue()) {
                                return Optional.of(lease());
                            } else {
                                if (logger.isInfoEnabled()) {
                                    logger.info("LEASE TERMINATED for volume {}", getVolume());
                                }
                                stopping(service.state());
                            }
                        }
                    }
                }
            } else {
                ListenableFuture<?> next;
                if (updateLease().isPresent()) {
                    next = Futures.immediateFuture(lease.get());
                } else {
                    try {
                        next = doLease();
                    } catch (Exception e) {
                        next = Futures.immediateFailedFuture(e);
                    }
                }
                return Optional.of(next);
            }
            return Optional.absent();
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("volume", getVolume()).toString();
        }
        
        protected class LeaseRequestListener extends ForwardingListenableFuture<Lease> implements Callable<Optional<Lease>>, Runnable {

            protected final SubmittedRequests<Records.Request,?> requests;
            protected final CallablePromiseTask<LeaseRequestListener,Lease> promise;
            
            public LeaseRequestListener(
                    Iterable<? extends Records.Request> requests) {
                this.requests = SubmittedRequests.submit(version.volume().volumes().getMaterializer(), requests);
                this.promise = CallablePromiseTask.create(this, SettableFuturePromise.<Lease>create());
                addListener(this, SameThreadExecutor.getInstance());
                this.requests.addListener(this, SameThreadExecutor.getInstance());
            }
            
            @Override
            public Optional<Lease> call() throws Exception {
                List<? extends Operation.ProtocolResponse<?>> responses = requests.get();
                for (int i=0; i<requests.requests().size(); ++i) {
                    Records.Request request = requests.requests().get(i);
                    Records.Response response = responses.get(i).record();
                    switch (request.opcode()) {
                    case CREATE:
                    case CREATE2:
                        Operations.maybeError(response, KeeperException.Code.NODEEXISTS);
                        break;
                    default:
                        Operations.unlessError(response);
                        break;
                    }
                }
                return Optional.of(updateLease().get());
            }

            @Override
            public void run() {
                if (!isDone()) {
                    delegate().run();
                } else {
                    Lease lease;
                    try {
                        lease = get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        // TODO Auto-generated catch block
                        logger.warn("", e);
                        throw Throwables.propagate(e);
                    }
                    if (logger.isInfoEnabled()) {
                        for (Records.Request request: requests.requests()) {
                            switch (request.opcode()) {
                            case SET_DATA:
                                logger.info("LEASE RENEWED {} for volume {}", lease, VersionedId.valueOf(version.getVersion(), version.volume().getVolume()));
                                break;
                            case CREATE:
                            case CREATE2:
                                logger.info("LEASE INITIALIZED {} for volume {}", lease, VersionedId.valueOf(version.getVersion(), version.volume().getVolume()));
                                break;
                            default:
                                break;
                            }
                        }
                    }
                }
            }

            @Override
            protected CallablePromiseTask<LeaseRequestListener,Lease> delegate() {
                return promise;
            }
        }
    }
}
