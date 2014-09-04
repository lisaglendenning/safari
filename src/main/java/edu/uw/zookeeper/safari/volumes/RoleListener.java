package edu.uw.zookeeper.safari.volumes;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.volumes.ScanLatestResidentLogs;
import edu.uw.zookeeper.safari.control.volumes.VolumeEntryAcceptors;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationCoordinator;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationCoordinatorEntry;
import edu.uw.zookeeper.safari.control.volumes.VolumesEntryCoordinator;
import edu.uw.zookeeper.safari.region.AbstractRoleListener;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.region.RegionRole;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.volumes.VolumeVersionCache;
import edu.uw.zookeeper.safari.storage.volumes.VolumesLeaseManager;
import edu.uw.zookeeper.safari.storage.volumes.XalphaCreator;

public final class RoleListener extends AbstractRoleListener<RoleListener> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public ListenerFactory newListenerFactory(
                @Region Identifier region,
                @Region AsyncFunction<VersionedId, Boolean> isResident,
                @Control Materializer<ControlZNode<?>,Message.ServerResponse<?>> control,
                @Storage Materializer<StorageZNode<?>,Message.ServerResponse<?>> storage,
                RegionRoleService.ListenersFactory listeners) {
            ListenerFactory instance = ListenerFactory.create(region, isResident, control, storage);
            listeners.get().add(instance);
            return instance;
        }
        
        @Provides @Singleton
        public RoleListener newRoleListener(
                final @edu.uw.zookeeper.safari.control.volumes.Volumes Function<Identifier, FutureTransition<UnsignedLong>> latest,
                final VolumeVersionCache.CachedVolumes volumes,
                final DirectoryEntryListener<StorageZNode<?>, StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
                final @edu.uw.zookeeper.safari.control.volumes.Volumes AsyncFunction<VersionedId, VolumeVersion<?>> branches,
                final @Region AsyncFunction<VersionedId, Boolean> isResident,
                final Provider<VolumeOperationExecutor<?,?>> operations,
                final SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control,
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage,
                final Supplier<FutureTransition<RegionRoleService>> role,
                final Configuration configuration,
                final ScheduledExecutorService scheduler,
                final ListenerFactory listeners,
                final @Region Service service) {
            return RoleListener.listen(
                    isResident, 
                    latest, 
                    branches, 
                    volumes, 
                    versions, 
                    operations, 
                    control, 
                    storage, 
                    configuration, 
                    scheduler, 
                    role, 
                    service);
        }

        @Override
        public Key<? extends Service.Listener> getKey() {
            return Key.get(RoleListener.class);
        }

        @Override
        protected void configure() {
        }
    }
    
    public static final class ListenerFactory implements Function<RegionRole, Iterable<? extends Service.Listener>> {
        
        public static <O extends Operation.ProtocolResponse<?>>  ListenerFactory create(
                final Identifier region, 
                final AsyncFunction<VersionedId, Boolean> isResident,
                final Materializer<ControlZNode<?>,O> control,
                final Materializer<StorageZNode<?>,O> storage) {
            final Supplier<LeaderListener> leader = LeaderListener.supplier(region, isResident, control, storage);
            return new ListenerFactory(
                    ImmutableMap.of(
                            EnsembleRole.LEADING, 
                            new Supplier<List<LeaderListener>>() {
                                @Override
                                public List<LeaderListener> get() {
                                    return ImmutableList.of(leader.get());
                                }
                            }));
        }
        
        private final ImmutableMap<EnsembleRole, ? extends Supplier<? extends Iterable<? extends Service.Listener>>> listeners;
        
        protected ListenerFactory(
                ImmutableMap<EnsembleRole, ? extends Supplier<? extends Iterable<? extends Service.Listener>>> listeners) {
            this.listeners = listeners;
        }
        
        @Override
        public Iterable<? extends Service.Listener> apply(RegionRole input) {
            Supplier<? extends Iterable<? extends Service.Listener>> listeners = this.listeners.get(input.getRole());
            if (listeners != null) {
                return listeners.get();
            } else {
                return ImmutableList.of();
            }
        }
    }

    public static final class LeaderListener extends LoggingServiceListener<LeaderListener> {

        public static <O extends Operation.ProtocolResponse<?>> Supplier<LeaderListener> supplier(
                final Identifier region, 
                final AsyncFunction<VersionedId, Boolean> isResident,
                final Materializer<ControlZNode<?>,O> control,
                final Materializer<StorageZNode<?>,O> storage) {
            final BootstrapRootVolume<?> root = BootstrapRootVolume.create(
                    region, 
                    control, 
                    storage);
            final Callable<Boolean> starting = new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Boolean bootstrapped = root.call();
                    if (!bootstrapped.booleanValue()) {
                        ScanLatestResidentLogs.call(
                                    isResident, 
                                    control).get();
                    }
                    return bootstrapped;
                }
            };
            return new Supplier<LeaderListener>() {
                @Override
                public LeaderListener get() {
                    return new LeaderListener(starting);
                }
            };
        }
        
        private final Callable<?> starting;
        
        protected LeaderListener(
                Callable<?> starting) {
            this.starting = starting;
        }
        
        @Override
        public void starting() {
            super.starting();
            try {
                starting.call();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    public static RoleListener listen(
            AsyncFunction<VersionedId, Boolean> isResident,
            Function<Identifier, FutureTransition<UnsignedLong>> latest,
            AsyncFunction<VersionedId, VolumeVersion<?>> branches,
            VolumeVersionCache.CachedVolumes volumes,
            DirectoryEntryListener<StorageZNode<?>, StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
            Provider<VolumeOperationExecutor<?,?>> operations,
            SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control,
            SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage,
            Configuration configuration,
            ScheduledExecutorService scheduler,
            Supplier<FutureTransition<RegionRoleService>> role,
            Service service) {
        return Services.listen(new RoleListener(
                isResident, latest, branches, volumes, versions, operations, control, storage, configuration, scheduler, role), service);
    }

    private final Function<Identifier, FutureTransition<UnsignedLong>> latest;
    private final VolumeVersionCache.CachedVolumes volumes;
    private final DirectoryEntryListener<StorageZNode<?>, StorageSchema.Safari.Volumes.Volume.Log.Version> versions;
    private final AsyncFunction<VersionedId, Boolean> isResident;
    private final AsyncFunction<VersionedId, VolumeVersion<?>> branches;
    private final Provider<VolumeOperationExecutor<?,?>> operations;
    private final SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control;
    private final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage;
    private final Configuration configuration;
    private final ScheduledExecutorService scheduler;
    
    protected RoleListener(
            AsyncFunction<VersionedId, Boolean> isResident,
            Function<Identifier, FutureTransition<UnsignedLong>> latest,
            AsyncFunction<VersionedId, VolumeVersion<?>> branches,
            VolumeVersionCache.CachedVolumes volumes,
            DirectoryEntryListener<StorageZNode<?>, StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
            Provider<VolumeOperationExecutor<?,?>> operations,
            SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control,
            SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage,
            Configuration configuration,
            ScheduledExecutorService scheduler,
            Supplier<FutureTransition<RegionRoleService>> role) {
        super(role);
        this.control = control;
        this.storage = storage;
        this.configuration = configuration;
        this.scheduler = scheduler;
        this.versions = versions;
        this.volumes = volumes;
        this.latest = latest;
        this.branches = branches;
        this.operations = operations;
        this.isResident = isResident;
    }

    @Override
    public void onSuccess(final RegionRoleService result) {
        super.onSuccess(result);
        if (result.getRole().getRole() == EnsembleRole.LEADING) {
            final VolumeOperationExecutor<?,?> operations = Services.listen(this.operations.get(), result);
            VolumesEntryCoordinator.listen(
                    isResident, 
                    new AsyncFunction<VolumeOperationCoordinatorEntry, Boolean>() {
                        @Override
                        public ListenableFuture<Boolean> apply(
                                VolumeOperationCoordinatorEntry input)
                                throws Exception {
                            return VolumeOperationCoordinator.forEntry(
                                            input, 
                                            operations,
                                            branches, 
                                            result, 
                                            control);
                        }
                    },  
                    control, 
                    result);
            VolumesLeaseManager.listen(
                    configuration, 
                    VolumeEntryAcceptors.listen(
                            control.materializer(), 
                            control.cacheEvents(), 
                            result),
                    scheduler, 
                    result,
                    storage);
            XalphaCreator.listen(
                    latest, 
                    // assumes that input is cached
                    new AsyncFunction<VersionedId, Optional<VersionedId>>() {
                        final LockableZNodeCache<ControlZNode<?>,?,?> cache = control.materializer().cache();
                        @Override
                        public ListenableFuture<Optional<VersionedId>> apply(
                                final VersionedId input) throws Exception {
                            Optional<VersionedId> result;
                            cache.lock().readLock().lock();
                            try {
                                ControlSchema.Safari.Volumes.Volume.Log.Version node = 
                                        ControlSchema.Safari.Volumes.Volume.Log.Version.fromTrie(cache.cache(), input.getValue(), input.getVersion());
                                Map.Entry<ZNodeName, ControlSchema.Safari.Volumes.Volume.Log.Version> lower = node.log().versions().lowerEntry(node.parent().name());
                                if (lower == null) {
                                    result = Optional.absent();
                                } else {
                                    result = Optional.of(lower.getValue().id());
                                }
                            } finally {
                                cache.lock().readLock().unlock();
                            }
                            return Futures.immediateFuture(result);
                        }
                    }, 
                    volumes, 
                    storage, 
                    result, 
                    versions);
        }
    }
}
