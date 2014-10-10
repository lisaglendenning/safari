package edu.uw.zookeeper.safari.volumes;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.VolumeDescriptor;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.volumes.CreateVolume;

/**
 *  synchronously create root volume and assign it to given region if there are no volumes
 *  note that if executed concurrently with other attempts to create the root volume, only one will succeed
 */
public final class BootstrapRootVolume<O extends Operation.ProtocolResponse<?>> implements Callable<Boolean> {
    
    public static <O extends Operation.ProtocolResponse<?>> BootstrapRootVolume<O> create(
            Identifier region, 
            Materializer<ControlZNode<?>,O> control,
            Materializer<StorageZNode<?>,O> storage) {
        return new BootstrapRootVolume<O>(
                region, 
                new Supplier<UnsignedLong>() {
                    @Override
                    public UnsignedLong get() {
                        return UnsignedLong.valueOf(System.currentTimeMillis());
                    }
                }, 
                control, 
                storage);
    }
    
    protected final Identifier region;
    protected final Supplier<UnsignedLong> version;
    protected final Materializer<ControlZNode<?>,O> control;
    protected final Materializer<StorageZNode<?>,O> storage;
    
    protected BootstrapRootVolume(
            Identifier region, 
            Supplier<UnsignedLong> version,
            Materializer<ControlZNode<?>,O> control,
            Materializer<StorageZNode<?>,O> storage) {
        this.region = region;
        this.version = version;
        this.control = control;
        this.storage = storage;
    }
    
    @Override
    public Boolean call() throws Exception {
        final VolumesSchemaRequests<O> schema = VolumesSchemaRequests.create(control);
        Pair<Boolean, VolumeDescriptor> root = LookupRootVolume.call(schema).get();
        Boolean bootstrapped = Boolean.FALSE;
        UnsignedLong version = this.version.get();
        if (root.first().booleanValue()) {
            bootstrapped = CreateRootVolume.call(schema, root.second(), version, region).get();
        }

        if (!bootstrapped.booleanValue()) {
            // if creating the root volume was unsuccessful, check if it exists and we are assigned to it
            Optional<UnsignedLong> latest = GetLatestVersion.call(schema, root.second().getId()).get();
            if (latest.isPresent()) {
                version = latest.get();
                Optional<RegionAndLeaves> state = GetState.call(schema.volume(root.second().getId()).version(version).state()).get();
                if (state.isPresent() && state.get().getRegion().equals(region)) {
                    bootstrapped = Boolean.TRUE;
                }
            }
        }

        // if the bootstrapped root volume exists and is assigned to us, then create the backend volume
        // we need to do this before the lease manager runs to maintain volume invariants
        if (bootstrapped.booleanValue()) {
            bootstrapped = CreateVolume.call(root.second().getId(), version, storage).get();
        }
        return bootstrapped;
    }
    
    protected static final class LookupRootVolume<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<List<O>, Pair<Boolean,VolumeDescriptor>> {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Pair<Boolean, VolumeDescriptor>> call(
                VolumesSchemaRequests<O> schema) {
            return Futures.transform(
                    SubmittedRequests.submit(schema.getMaterializer(), schema.children()), 
                    new BootstrapRootVolume.LookupRootVolume<O>(schema.getMaterializer().cache()));
        }
        
        private final LockableZNodeCache<ControlZNode<?>,?,O> cache;
        
        protected LookupRootVolume(
                LockableZNodeCache<ControlZNode<?>,?,O> cache) {
            this.cache = cache;
        }
        
        @Override
        public ListenableFuture<Pair<Boolean, VolumeDescriptor>> apply(List<O> input) throws Exception {
            for (O response: input) {
                Operations.unlessError(response.record());
            }

            final VolumeDescriptor root;
            final boolean emptyVolumes;
            cache.lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes volumes = ControlSchema.Safari.Volumes.fromTrie(cache.cache());
                root = VolumeDescriptor.valueOf(volumes.hasher().apply(ZNodePath.root()).asIdentifier(), ZNodePath.root());
                emptyVolumes = volumes.isEmpty();
            } finally {
                cache.lock().readLock().unlock();
            }
            return Futures.immediateFuture(
                    Pair.create(Boolean.valueOf(emptyVolumes), root));
        }   
    }
        
    protected static final class CreateRootVolume implements Function<Operation.ProtocolResponse<?>, Boolean> {
        
        public static ListenableFuture<Boolean> call(
                final VolumesSchemaRequests<?> schema,
                final VolumeDescriptor root,
                final UnsignedLong version,
                final Identifier region) {
            VolumesSchemaRequests<?>.VolumeSchemaRequests volume = schema.volume(root.getId());
            ImmutableList.Builder<Records.MultiOpRequest> multi = ImmutableList.builder();
            multi.addAll(volume.create(root.getPath()));
            VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests requests = volume.version(version);
            multi.addAll(requests.create(Optional.of(RegionAndLeaves.empty(region))));
            multi.add(requests.latest().create());
            return Futures.transform(
                    schema.getMaterializer().submit(new IMultiRequest(multi.build())), 
                    new CreateRootVolume());
        }
        
        protected CreateRootVolume() {
        }

        @Override
        public Boolean apply(Operation.ProtocolResponse<?> input) {
            try {
                Operations.unlessMultiError((IMultiResponse) input.record());
                return Boolean.TRUE;
            } catch (KeeperException e) {
            }
            return Boolean.FALSE;
        }
    }

    protected static final class GetLatestVersion<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<List<O>, Optional<UnsignedLong>> {
        
        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<UnsignedLong>> call(
                final VolumesSchemaRequests<O> schema,
                final Identifier id) {
            return Futures.transform(
                    SubmittedRequests.submit(schema.getMaterializer(), schema.volume(id).latest().get()), 
                    new BootstrapRootVolume.GetLatestVersion<O>(id, schema.getMaterializer().cache()));
        }

        private final Identifier id;
        private final LockableZNodeCache<ControlZNode<?>,?,O> cache;
        
        protected GetLatestVersion(
                Identifier id,
                LockableZNodeCache<ControlZNode<?>,?,O> cache) {
            this.id = id;
            this.cache = cache;
        }

        @Override
        public ListenableFuture<Optional<UnsignedLong>> apply(List<O> input) throws Exception {
            final Optional<UnsignedLong> value;
            Optional<Operation.Error> error = null;
            for (O response: input) {
                error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
            }
            if (error.isPresent()) {
                value = Optional.absent();
            } else {
                cache.lock().readLock().lock();
                try {
                    value = Optional.fromNullable(ControlSchema.Safari.Volumes.Volume.fromTrie(cache.cache(), id).getLog().latest().data().get());
                } finally {
                    cache.lock().readLock().unlock();
                }
            }
            return Futures.immediateFuture(value);
        }
    }

    protected static final class GetState<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<List<O>, Optional<RegionAndLeaves>> {
        
        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<RegionAndLeaves>> call(
                final VolumesSchemaRequests<O>.VolumeSchemaRequests.VolumeVersionSchemaRequests.VolumeStateSchemaRequests schema) {
            Materializer<ControlZNode<?>,O> materializer = schema.version().volume().volumes().getMaterializer();
            return Futures.transform(
                    SubmittedRequests.submit(materializer, schema.get()), 
                    new BootstrapRootVolume.GetState<O>(schema.getPath(), materializer.cache()));
        }

        private final ZNodePath path;
        private final LockableZNodeCache<ControlZNode<?>,?,O> cache;
        
        protected GetState(
                ZNodePath path,
                LockableZNodeCache<ControlZNode<?>,?,O> cache) {
            this.path = path;
            this.cache = cache;
        }

        @Override
        public ListenableFuture<Optional<RegionAndLeaves>> apply(List<O> input) throws Exception {
            final Optional<RegionAndLeaves> value;
            Optional<Operation.Error> error = null;
            for (O response: input) {
                error = Operations.maybeError(response.record());
            }
            if (error.isPresent()) {
                value = Optional.absent();
            } else {
                cache.lock().readLock().lock();
                try {
                    value = Optional.fromNullable((RegionAndLeaves) cache.cache().get(path).data().get());
                } finally {
                    cache.lock().readLock().unlock();
                }
            }
            return Futures.immediateFuture(value);
        }
    }
}