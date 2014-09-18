package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
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

import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.region.InjectingRoleListener;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.DirectoryWatcherService;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeState;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

public final class RoleListener implements Function<RegionRoleService, List<? extends com.google.inject.Module>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}

        @Provides @Region 
        public Predicate<AssignedVolumeState> isStateAssigned(
                final @Region Identifier region) {
            return new Predicate<AssignedVolumeState>() {
                @Override
                public boolean apply(AssignedVolumeState input) {
                    return region.equals(input.getRegion());
                }
            };
        }
        
        @Provides @Region 
        public Function<VolumeVersion<?>, Boolean> isStateResident(
                final @Region Predicate<AssignedVolumeState> isAssigned) {
            return new Function<VolumeVersion<?>, Boolean>() {
                @Override
                public Boolean apply(VolumeVersion<?> input) {
                    return ((input instanceof AssignedVolumeBranches) && isAssigned.apply((((AssignedVolumeBranches) input).getState().getValue())));
                }
            };
        }
        
        @Provides @Region 
        public AsyncFunction<VersionedId, Boolean> isVersionResident(
                final @Region Function<VolumeVersion<?>, Boolean> isResident,
                final @Volumes AsyncFunction<VersionedId, VolumeVersion<?>> versions) {
            return new AsyncFunction<VersionedId, Boolean>() {
                @Override
                public ListenableFuture<Boolean> apply(VersionedId input)
                        throws Exception {
                    return Futures.transform(
                            versions.apply(input), 
                            isResident);
                }
            };
        }

        @Provides @Singleton
        public InjectingRoleListener<RoleListener> newRoleListener(
                Injector injector,
                Supplier<FutureTransition<RegionRoleService>> role,
                DirectoryWatcherService<ControlSchema.Safari.Volumes> service) {
            return RoleListener.listen(injector, role, service);
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
                    ResidentLogListener.module(),
                    OutdatedEntryRejecter.module());
        default:
            return ImmutableList.of();
        }
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }
}
