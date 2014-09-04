package edu.uw.zookeeper.safari.control.volumes;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.region.AbstractRoleListener;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.DirectoryWatcherService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeState;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

public final class RoleListener extends AbstractRoleListener<RoleListener> {

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
                            isResident, 
                            SameThreadExecutor.getInstance());
                }
            };
        }

        @Provides @Singleton
        public RoleListener newRoleListener(
                @Region Predicate<AssignedVolumeState> isAssigned,
                @Region AsyncFunction<VersionedId, Boolean> isResident,
                SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control,
                Supplier<FutureTransition<RegionRoleService>> role,
                DirectoryWatcherService<ControlSchema.Safari.Volumes> service) {
            // FIXME start services
            return RoleListener.listen(isAssigned, isResident, control, role, service);
        }

        @Override
        public Key<? extends Service.Listener> getKey() {
            return Key.get(RoleListener.class);
        }
        
        @Override
        protected void configure() {
        }
    }
    
    public static RoleListener listen(
            Predicate<AssignedVolumeState> isAssigned,
            AsyncFunction<VersionedId, Boolean> isResident,
            SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control,
            Supplier<FutureTransition<RegionRoleService>> role,
            Service service) {
        return Services.listen(new RoleListener(isAssigned, isResident, control, role), service);
    }

    private final Predicate<AssignedVolumeState> isAssigned;
    private final AsyncFunction<VersionedId, Boolean> isResident;
    private final SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control;
    
    protected RoleListener(
            Predicate<AssignedVolumeState> isAssigned,
            AsyncFunction<VersionedId, Boolean> isResident,
            SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> control,
            Supplier<FutureTransition<RegionRoleService>> role) {
        super(role);
        this.isAssigned = isAssigned;
        this.isResident = isResident;
        this.control = control;
    }

    @Override
    public void onSuccess(final RegionRoleService result) {
        super.onSuccess(result);
        if (result.getRole().getRole() == EnsembleRole.LEADING) {
            ResidentLogListener.listen(
                    isAssigned, 
                    control, 
                    result, 
                    result.logger());
            OutdatedEntryRejecter.listen(
                    isResident, 
                    control, 
                    result, 
                    result.logger());
        }
    }
}
