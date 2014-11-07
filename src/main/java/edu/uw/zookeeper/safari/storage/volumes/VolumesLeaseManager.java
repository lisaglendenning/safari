package edu.uw.zookeeper.safari.storage.volumes;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Lease;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Assumes volume versions, xalpha, xomega are being watched.
 */
public final class VolumesLeaseManager extends Watchers.CacheNodeCreatedListener<StorageZNode<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}
        
        @Provides @Singleton
        public VolumesLeaseManager getVolumesLeaseManager(
                final SchemaClientService<StorageZNode<?>,?> client,
                final Configuration configuration,
                final @Volumes AsyncFunction<VersionedId, Boolean> renewal,
                final ScheduledExecutorService scheduler,
                final RegionRoleService service) {
            return VolumesLeaseManager.listen(
                    configuration,
                    renewal,
                    client,
                    scheduler, 
                    service);
        }

        @Override
        public Key<? extends Service.Listener> getKey() {
            return Key.get(VolumesLeaseManager.class);
        }

        @Override
        protected void configure() {
        }
    }
    
    public static VolumesLeaseManager listen(
            Configuration configuration,
            AsyncFunction<? super VersionedId, Boolean> renewal,
            SchemaClientService<StorageZNode<?>,?> client,
            ScheduledExecutorService scheduler,
            Service service) {
        return listen(
                Functions.constant(
                        UnsignedLong.valueOf(
                                ConfigurableLeaseDuration.get(
                                        configuration)
                                    .value(TimeUnit.MILLISECONDS))),
                renewal,
                client,
                scheduler,
                service);
    }
    
    public static VolumesLeaseManager listen(
            Function<? super Identifier, UnsignedLong> durations,
            AsyncFunction<? super VersionedId, Boolean> renewal,
            SchemaClientService<StorageZNode<?>,?> client,
            ScheduledExecutorService scheduler,
            Service service) {
        final ConcurrentMap<Identifier, VolumeLeaseManager> managers = new MapMaker().makeMap();
        VolumesLeaseManager instance = new VolumesLeaseManager(
                    durations,
                    renewal,
                    managers,
                    client.materializer(),
                    scheduler,
                    service, 
                    client.cacheEvents());
        instance.listen();
        XomegaListener.listen(
                managers, 
                instance, 
                client.materializer().cache(), 
                service, 
                client.cacheEvents(), 
                instance.logger());
        return instance;
    }
    
    @Configurable(arg="lease", path="volume", key="lease", value="60 seconds", help="time")
    public static abstract class ConfigurableLeaseDuration {

        public static Configurable getConfigurable() {
            return ConfigurableLeaseDuration.class.getAnnotation(Configurable.class);
        }
        
        public static TimeValue get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            return TimeValue.fromString(
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key()));
        }

        public static Configuration set(Configuration configuration, TimeValue value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(ConfigFactory.parseMap(ImmutableMap.<String,Object>builder().put(ConfigUtil.joinPath(configurable.path(), configurable.key()), value.toString()).build()));
        }
        
        protected ConfigurableLeaseDuration() {}
    }
    
    protected final ScheduledExecutorService scheduler;
    protected final Materializer<StorageZNode<?>,?> materializer;
    protected final Function<? super Identifier, UnsignedLong> durations;
    protected final AsyncFunction<? super VersionedId, Boolean> renewal;
    protected final ConcurrentMap<Identifier, VolumeLeaseManager> managers;
    
    protected VolumesLeaseManager(
            Function<? super Identifier, UnsignedLong> durations,
            AsyncFunction<? super VersionedId, Boolean> renewal,
            ConcurrentMap<Identifier, VolumeLeaseManager> managers,
            Materializer<StorageZNode<?>,?> materializer,
            ScheduledExecutorService scheduler,
            Service service,
            WatchListeners cacheEvents) {
        super(materializer.cache(), 
                service, 
                cacheEvents,
                WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha.PATH,
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDeleted));
        this.scheduler = scheduler;
        this.materializer = materializer;
        this.durations = durations;
        this.renewal = renewal;
        this.managers = managers;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        super.handleWatchEvent(event);
        final StorageSchema.Safari.Volumes.Volume.Log.Version parent = (StorageSchema.Safari.Volumes.Volume.Log.Version) cache.cache().get(((AbsoluteZNodePath) event.getPath()).parent());
        switch (event.getEventType()) {
        case NodeCreated:
        {
            final Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> last = parent.log().versions().lastEntry();
            if ((parent.xomega() == null) && ((parent == last.getValue()) || (parent == parent.log().versions().lowerEntry(last.getKey()).getValue()))) {
                final VersionedId version = parent.id();
                VolumeLeaseManager manager = managers.get(version.getValue());
                if (manager != null) {
                    if (version.getVersion().longValue() <= manager.getVolume().getVersion().longValue()) {
                        return;
                    }
                    manager.stopping(state());
                    manager.terminated(state());
                }
                manager = new VolumeLeaseManager(version, durations.apply(version.getValue()));
                VolumeLeaseManager existing = managers.putIfAbsent(version.getValue(), manager);
                assert (existing == null);
                manager.starting();
                manager.running();
                switch (state()) {
                case STOPPING:
                    manager.stopping(Service.State.RUNNING);
                    break;
                case TERMINATED:
                    manager.stopping(Service.State.RUNNING);
                    manager.terminated(Service.State.STOPPING);
                    break;
                case FAILED:
                    manager.failed(Service.State.RUNNING, service.failureCause());
                    break;
                default:
                    break;
                }
            }
            break;
        }
        case NodeDeleted:
        {
            final VersionedId volume;
            if (parent != null) {
                volume = parent.id();
            } else {
                AbsoluteZNodePath path = (AbsoluteZNodePath) ((AbsoluteZNodePath) event.getPath()).parent();
                UnsignedLong version = UnsignedLong.valueOf(path.label().toString());
                path = (AbsoluteZNodePath) ((AbsoluteZNodePath) path.parent()).parent();
                Identifier id = Identifier.valueOf(path.label().toString());
                volume = VersionedId.valueOf(version, id);
            }
            VolumeLeaseManager manager = managers.get(volume.getValue());
            if (manager != null) {
                if (manager.getVolume().getVersion().equals(volume.getVersion())) {
                    manager.stopping(Service.State.RUNNING);
                    manager.terminated(Service.State.STOPPING);
                }
            }
            break;
        }
        default:
            break;
        }
    }
    
    @Override
    public void starting() {
        logger.debug("STARTING ({})", delegate);
    }
    
    @Override
    public void running() {
        getWatch().subscribe(this);
        super.running();
    }
    
    @Override
    public void stopping(Service.State from) {
        super.stopping(from);
        for (VolumeLeaseManager manager: managers.values()) {
            manager.stopping(from);
        }
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        for (VolumeLeaseManager manager: Iterables.consumingIterable(managers.values())) {
            manager.terminated(from);
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        for (VolumeLeaseManager manager: Iterables.consumingIterable(managers.values())) {
            manager.failed(from, failure);
        }
        Services.stop(service);
    }
    
    protected static final class XomegaListener extends Watchers.FailWatchListener<StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega> {

        public static Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                ConcurrentMap<Identifier, VolumeLeaseManager> managers,
                WatchMatchServiceListener listener,
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                Service service,
                WatchListeners cacheEvents,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.listen(
                    cache, 
                    service, 
                    cacheEvents,
                    Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        new XomegaListener(managers, listener), 
                                        cache.cache())),
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.PATH, 
                                Watcher.Event.EventType.NodeCreated),
                        logger), 
                    logger);
        }
        
        private final ConcurrentMap<Identifier, VolumeLeaseManager> managers;
        
        protected XomegaListener(
                ConcurrentMap<Identifier, VolumeLeaseManager> managers,
                WatchMatchServiceListener listener) {
            super(listener);
            this.managers = managers;
        }

        @Override
        public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega result) {
            if (result != null) {
                VolumeLeaseManager manager = managers.get(result.version().log().volume().name());
                if (manager != null) {
                    if (manager.getVolume().getVersion().equals(result.version().name())) {
                        manager.stopping(Service.State.RUNNING);
                        manager.terminated(Service.State.STOPPING);
                    }
                }
            }
        }
    }
    
    protected final class VolumeLeaseManager extends LoggingServiceListener<VolumeLeaseManager> implements Runnable {
        
        private final VersionedId volume;
        private final UnsignedLong duration;
        private final AbsoluteZNodePath path;
        private Optional<Pair<Integer, Lease>> lease;
        private Optional<? extends ListenableFuture<?>> future;
        
        protected VolumeLeaseManager(
                VersionedId volume,
                UnsignedLong duration) {
            super();
            this.volume = volume;
            this.path = StorageSchema.Safari.Volumes.Volume.Log.Version.Lease.pathOf(volume.getValue(), volume.getVersion());
            this.duration = duration;
            this.lease = Optional.absent();
            this.future = Optional.absent();
        }
        
        public AbsoluteZNodePath getPath() {
            return path;
        }
        
        public VersionedId getVolume() {
            return volume;
        }
        
        public Optional<Pair<Integer, Lease>> getCurrentLease() {
            cache.lock().readLock().lock();
            try {
                StorageSchema.Safari.Volumes.Volume.Log.Version.Lease node = (StorageSchema.Safari.Volumes.Volume.Log.Version.Lease) cache.cache().get(
                        getPath()); 
                if ((node != null) && (node.stat().get() != null) && (node.data().get() != null) && (node.stat().stamp() == node.data().stamp())) { 
                    return Optional.of(
                            Pair.create(
                                    Integer.valueOf(node.stat().get().getVersion()), 
                                    Lease.valueOf(node.stat().get().getMtime(), node.data().get().longValue())));
                } else {
                    return Optional.absent();
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        public UnsignedLong getDuration() {
            return duration;
        }
        
        @Override
        public synchronized void run() {
            switch (state()) {
            case NEW:
            case STARTING:
            case RUNNING:
            {
                Optional<? extends ListenableFuture<?>> next;
                try {
                    next = call();
                } catch (Exception e) {
                    failed(state(), e);
                    return;
                }
                if (next.isPresent()) {
                    future = next;
                    future.get().addListener(this, MoreExecutors.directExecutor());
                }
                break;
            }
            default:
                break;
            }
        }
        
        @Override
        public void running() {
            super.running();
            run();
        }

        @Override
        public synchronized void terminated(Service.State from) {
            super.terminated(from);
            stop();
        }

        @Override
        public synchronized void failed(Service.State from, Throwable failure) {
            super.failed(from, failure);
            stop();
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("volume", getVolume()).toString();
        }
        
        protected synchronized void stop() {
            managers.remove(getVolume().getValue(), this);
            if (future.isPresent() && !future.get().isDone()) {
                future.get().cancel(false);
            }
        }

        protected ListenableFuture<Boolean> doLease() {
            if (this == managers.get(volume.getValue())) {
                try {
                    return Futures.nonCancellationPropagating(
                            renewal.apply(getVolume()));
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            } else {
                return Futures.immediateCancelledFuture();
            }
        }
        
        @SuppressWarnings("unchecked")
        protected ListenableFuture<Pair<Integer,Lease>> lease() {
            final AbsoluteZNodePath path = getPath();
            final ImmutableList.Builder<Records.Request> requests = ImmutableList.<Records.Request>builder();
            if (lease.isPresent()) {
                Operations.Requests.SerializedData<?, Operations.Requests.SetData, ?> builder = materializer.setData(path, getDuration()).get();
                builder.delegate().setVersion(lease.get().first().intValue());
                requests.add(builder.build());
            } else {
                requests.add(materializer.create(path, getDuration()).get().build());
                requests.addAll(PathToRequests.forRequests(
                        Operations.Requests.sync(), 
                        Operations.Requests.getData()).apply(path));
            }
            return LoggingFutureListener.listen(
                    logger, 
                    new LeaseRequestListener(SubmittedRequests.submit(materializer, requests.build())));
        }
        
        protected synchronized Optional<Pair<Integer, Lease>> updateLease() {
            lease = getCurrentLease();
            return lease;
        }
        
        protected Optional<? extends ListenableFuture<?>> call() throws Exception {
            if (future.isPresent()) {
                if (future.get().isDone()) {
                    if (future.get().isCancelled()) {
                        stopping(State.RUNNING);
                    } else {
                        Object result = future.get().get();
                        if (result instanceof Pair) {
                            final Lease lease = (Lease) ((Pair<?,?>) result).second();
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
        
        private final class LeaseRequestListener extends ToStringListenableFuture<Pair<Integer,Lease>> implements Callable<Optional<Pair<Integer,Lease>>>, Runnable {

            private final SubmittedRequests<Records.Request,?> requests;
            private final CallablePromiseTask<LeaseRequestListener,Pair<Integer,Lease>> promise;
            
            protected LeaseRequestListener(
                    SubmittedRequests<Records.Request,?> requests) {
                this.requests = requests;
                this.promise = CallablePromiseTask.create(this, SettableFuturePromise.<Pair<Integer,Lease>>create());
                addListener(this, MoreExecutors.directExecutor());
                this.requests.addListener(this, MoreExecutors.directExecutor());
            }
            
            @Override
            public Optional<Pair<Integer,Lease>> call() throws Exception {
                if (requests.isDone()) {
                    List<? extends Operation.ProtocolResponse<?>> responses = requests.get();
                    for (int i=0; i<requests.getValue().size(); ++i) {
                        Records.Request request = requests.getValue().get(i);
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
                return Optional.absent();
            }

            @Override
            public void run() {
                if (!isDone()) {
                    delegate().run();
                } else if (!isCancelled()) {
                    Pair<Integer,Lease> lease;
                    try {
                        lease = get();
                    } catch (Exception e) {
                        return;
                    }
                    if (logger.isInfoEnabled()) {
                        for (Records.Request request: requests.getValue()) {
                            switch (request.opcode()) {
                            case SET_DATA:
                                logger.info("LEASE RENEWED {} for volume {}", lease.second(), getVolume());
                                break;
                            case CREATE:
                            case CREATE2:
                                logger.info("LEASE INITIALIZED {} for volume {}", lease.second(), getVolume());
                                break;
                            default:
                                break;
                            }
                        }
                    }
                }
            }

            @Override
            protected CallablePromiseTask<LeaseRequestListener,Pair<Integer,Lease>> delegate() {
                return promise;
            }
        }
    }
}
