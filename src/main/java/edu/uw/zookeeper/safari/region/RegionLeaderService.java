package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.safari.storage.ExpireSnapshotSessions;
import edu.uw.zookeeper.safari.storage.CleanSnapshot;
import edu.uw.zookeeper.safari.storage.StorageClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.volume.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.volume.EmptyVolume;
import edu.uw.zookeeper.safari.volume.RegionAndBranches;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;
import edu.uw.zookeeper.safari.volume.VolumeDescriptor;
import edu.uw.zookeeper.safari.volume.VolumeVersion;

public class RegionLeaderService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Provides
        public RegionLeaderService newRegionLeaderService(
                @Region Identifier id,
                TaskExecutor<VolumeOperationDirective, Boolean> executor,
                VolumeCacheService volumes,
                ControlClientService control,
                StorageClientService storage,
                ScheduledExecutorService scheduler,
                Configuration configuration) {
            RegionLeaderService instance = RegionLeaderService.create(
                    id,
                    executor,
                    volumes,
                    control, 
                    storage,
                    scheduler,
                    configuration);
            return instance;
        }

        @Override
        protected void configure() {
            install(VolumeOperationExecutor.module());
        }
    }
    
    public static RegionLeaderService create(
            final Identifier region, 
            final TaskExecutor<VolumeOperationDirective,Boolean> executor,
            final VolumeCacheService volumes,
            final ControlClientService control, 
            final StorageClientService storage,
            final ScheduledExecutorService scheduler,
            final Configuration configuration) {
        final Predicate<Identifier> isAssigned = Predicates.equalTo(region);
        RegionLeaderService instance = new RegionLeaderService(ImmutableList.of(
                new Service.Listener() {
                    @Override
                    public void starting() {
                        Services.startAndWait(volumes);
                    }
                },
                new BootstrapRootVolume(region, control.materializer(), storage.materializer())));
        CleanSnapshot.listen(instance, storage);
        ExpireSnapshotSessions.listen(instance, storage);
        VolumesEntryCoordinator.listen(
                isAssigned, 
                executor, 
                volumes.idToPath().asLookup().cached(), 
                new Function<VersionedId, Optional<RegionAndBranches>>() {
                    @Override
                    public Optional<RegionAndBranches> apply(
                            VersionedId input) {
                        // TODO look up older versions from cache
                        // (but this shouldn't be necessary)
                        VolumeVersion<?> volume = volumes.idToVolume().cached().apply(input.getValue());
                        assert (volume.getState().getVersion().equals(input.getVersion()));
                        if (volume instanceof EmptyVolume) {
                            return Optional.absent();
                        } else {
                            return Optional.of(((AssignedVolumeBranches) volume).getState().getValue());
                        }
                    }
                }, 
                control, 
                instance);
        OutdatedVolumeEntryVoter.listen(isAssigned, control, instance);
        VolumeStateListener.listen(
                VolumeEntriesWatcher.fromState(
                        VolumeEntriesWatcher.regionEquals(region), 
                        instance, 
                        control), 
                control.materializer().cache(), 
                instance, 
                control.cacheEvents());
        VolumesLeaseManager.defaults(
                region, 
                scheduler, 
                instance, 
                control, 
                storage,
                configuration);
        return instance;
    }
    
    protected RegionLeaderService(
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
    }
    
    @Override
    protected void startUp() throws Exception {
        logger.info("STARTING {}", this);
        super.startUp();
    }
    
    public static class BootstrapRootVolume extends Service.Listener {
        
        protected final Identifier region;
        protected final Materializer<ControlZNode<?>,?> control;
        protected final Materializer<StorageZNode<?>,?> storage;
        
        public BootstrapRootVolume(
                Identifier region, 
                Materializer<ControlZNode<?>,?> control,
                Materializer<StorageZNode<?>,?> storage) {
            this.region = region;
            this.control = control;
            this.storage = storage;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public void starting() {
            // bootstrap system: synchronously create root volume and assign it to myself if there are no volumes
            // note that if executed concurrently with other attempts to create the root volume, only one will succeed
            final VolumesSchemaRequests<?> schema = VolumesSchemaRequests.create(control);
            try {
                List<? extends Operation.ProtocolResponse<?>> responses = SubmittedRequests.submit(control, schema.children()).get();
                for (Operation.ProtocolResponse<?> response: responses) {
                    Operations.unlessError(response.record());
                }
                final VolumeDescriptor root;
                final boolean emptyVolumes;
                control.cache().lock().readLock().lock();
                try {
                    ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(control.cache().cache());
                    root = VolumeDescriptor.valueOf(volumes.hasher().apply(ZNodePath.root()).asIdentifier(), ZNodePath.root());
                    emptyVolumes = volumes.isEmpty();
                } finally {
                    control.cache().lock().readLock().unlock();
                }
                boolean bootstrapped = false;
                if (emptyVolumes) {
                    VolumesSchemaRequests.VolumeSchemaRequests volume = schema.volume(root.getId());
                    ImmutableList.Builder<Records.MultiOpRequest> multi = ImmutableList.builder();
                    multi.addAll(volume.create(root.getPath()));
                    VolumesSchemaRequests.VolumeSchemaRequests.VolumeVersionSchemaRequests version = volume.version(UnsignedLong.valueOf(System.currentTimeMillis()));
                    multi.addAll(version.create(Optional.of(RegionAndLeaves.empty(region))));
                    multi.add(version.latest().create());
                    IMultiResponse response = (IMultiResponse) control.submit(new IMultiRequest(multi.build())).get().record();
                    Operations.unlessError(response);
                    try {
                        Operations.unlessMultiError(response);
                        bootstrapped = true;
                    } catch (KeeperException e) {
                    }
                }

                // if the bootstrapped root volume exists and is assigned to us, then create the backend root znode
                // we need to do this before the lease manager runs to maintain volume invariants
                if (!bootstrapped) {
                    try {
                        responses = SubmittedRequests.submit(control, schema.volume(root.getId()).latest().get()).get();
                        for (Operation.ProtocolResponse<?> response: responses) {
                            Operations.unlessError(response.record());
                        }
                        Optional<UnsignedLong> version = Optional.absent();
                        control.cache().lock().readLock().lock();
                        try {
                            ControlSchema.Safari.Volumes.Volume.Log.Latest latest = ControlSchema.Safari.Volumes.Volume.fromTrie(control.cache().cache(), root.getId()).getLog().latest();
                            version = Optional.fromNullable(latest.data().get());
                        } finally {
                            control.cache().lock().readLock().unlock();
                        }
                        if (version.isPresent()) {
                            responses = SubmittedRequests.submit(control, schema.volume(root.getId()).version(version.get()).state().get()).get();
                            for (Operation.ProtocolResponse<?> response: responses) {
                                Operations.unlessError(response.record());
                            }
                            control.cache().lock().readLock().lock();
                            try {
                                ControlSchema.Safari.Volumes.Volume.Log.Version.State state = ControlSchema.Safari.Volumes.Volume.Log.Version.fromTrie(control.cache().cache(), root.getId(), version.get()).state();
                                if ((state.data().get() != null) && state.data().get().getRegion().equals(region)) {
                                    bootstrapped = true;
                                }
                            } finally {
                                control.cache().lock().readLock().unlock();
                            }
                        }
                    } catch (KeeperException e) {}
                }
                if (bootstrapped) {
                    responses = SubmittedRequests.submitRequests(
                            storage,
                            Operations.Requests.create().setPath(StorageSchema.Safari.Volumes.Volume.pathOf(root.getId())).build(),
                            Operations.Requests.create().setPath(StorageSchema.Safari.Volumes.Volume.Root.pathOf(root.getId())).build()).get();
                    for (Operation.ProtocolResponse<?> response: responses) {
                        Operations.maybeError(
                                response.record(),
                                KeeperException.Code.NODEEXISTS);
                    }
                }
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
