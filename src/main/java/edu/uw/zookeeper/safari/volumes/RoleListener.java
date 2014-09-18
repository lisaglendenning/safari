package edu.uw.zookeeper.safari.volumes;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.LoggingServiceListener;
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
import edu.uw.zookeeper.safari.control.volumes.VolumesEntryCoordinator;
import edu.uw.zookeeper.safari.region.InjectingRoleListener;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.region.RegionRole;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeState;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.volumes.VolumesLeaseManager;
import edu.uw.zookeeper.safari.storage.volumes.XalphaCreator;

public final class RoleListener implements Function<RegionRoleService, List<? extends com.google.inject.Module>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public ListenerFactory newListenerFactory(
                @Region Identifier region,
                @Region Predicate<AssignedVolumeState> isAssigned,
                @Control Materializer<ControlZNode<?>,Message.ServerResponse<?>> control,
                @Storage Materializer<StorageZNode<?>,Message.ServerResponse<?>> storage,
                RegionRoleService.ListenersFactory listeners) {
            ListenerFactory instance = ListenerFactory.create(region, isAssigned, control, storage);
            listeners.get().add(instance);
            return instance;
        }
        
        @Provides @Singleton
        public InjectingRoleListener<RoleListener> newRoleListener(
                ListenerFactory listeners,
                final Supplier<FutureTransition<RegionRoleService>> role,
                final Injector injector,
                final @Region Service service) {
            return RoleListener.listen(
                    injector,
                    role,
                    service);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(InjectingRoleListener.class, Volumes.class);
        }

        @Override
        protected void configure() {
            bind(Key.get(InjectingRoleListener.class, Volumes.class)).to(Key.get(new TypeLiteral<InjectingRoleListener<RoleListener>>(){}));
        }
    }
    
    public static final class ListenerFactory implements Function<RegionRole, Iterable<? extends Service.Listener>> {
        
        public static <O extends Operation.ProtocolResponse<?>>  ListenerFactory create(
                final Identifier region, 
                final Predicate<AssignedVolumeState> isAssigned,
                final Materializer<ControlZNode<?>,O> control,
                final Materializer<StorageZNode<?>,O> storage) {
            final Supplier<LeaderListener> leader = LeaderListener.supplier(region, isAssigned, control, storage);
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
                final Predicate<AssignedVolumeState> isAssigned,
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
                                    isAssigned, 
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

    public static InjectingRoleListener<RoleListener> listen(
            Injector injector,
            Supplier<FutureTransition<RegionRoleService>> role,
            Service service) {
        final RoleListener instance = new RoleListener();
        return InjectingRoleListener.listen(
                injector, 
                instance, 
                role, 
                instance, 
                LogManager.getLogger(instance),
                service);
    }
    
    protected RoleListener() {}

    @Override
    public List<? extends com.google.inject.Module> apply(final RegionRoleService input) {
        switch (input.getRole().getRole()) {
        case LEADING:
            return ImmutableList.of(
                    InjectingRoleListener.RegionRoleServiceModule.create(input),
                    VolumeOperationExecutor.module(),
                    VolumesEntryCoordinator.module(),
                    VolumeEntryAcceptors.module(),
                    new AbstractModule() {
                        @Override
                        protected void configure() {
                            bind(Key.get(new TypeLiteral<AsyncFunction<VersionedId, Boolean>>(){}, edu.uw.zookeeper.safari.storage.volumes.Volumes.class)).to(Key.get(new TypeLiteral<AsyncFunction<VersionedId, Boolean>>(){}, edu.uw.zookeeper.safari.control.volumes.Volumes.class));
                        }
                    },
                    VolumesLeaseManager.module(),
                    new XalphaCreatorModule(),
                    XalphaCreator.module());
        default:
            return ImmutableList.of();
        }
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }
    
    protected static class XalphaCreatorModule extends AbstractModule {
        
        protected XalphaCreatorModule() {
        }
        
        @Provides @Singleton @edu.uw.zookeeper.safari.storage.volumes.Volumes
        public AsyncFunction<VersionedId, Optional<VersionedId>> get(
                final @Control Materializer<ControlZNode<?>,?> control) {
            // TODO subscribe at running (?)
            // or fill in async part // assumes that input is cached
            return new AsyncFunction<VersionedId, Optional<VersionedId>>() {
                final LockableZNodeCache<ControlZNode<?>,?,?> cache = control.cache();
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
            };
        }

        @Override
        protected void configure() {
            bind(Key.get(new TypeLiteral<Function<Identifier, FutureTransition<UnsignedLong>>>(){}, edu.uw.zookeeper.safari.storage.volumes.Volumes.class)).to(Key.get(new TypeLiteral<Function<Identifier, FutureTransition<UnsignedLong>>>(){}, edu.uw.zookeeper.safari.control.volumes.Volumes.class));
        }
    }
}
