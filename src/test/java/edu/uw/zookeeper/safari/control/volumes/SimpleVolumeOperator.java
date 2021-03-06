package edu.uw.zookeeper.safari.control.volumes;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedValue;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.volumes.LatestVolumeCache;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeOperator;
import edu.uw.zookeeper.safari.schema.volumes.EmptyVolume;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndBranches;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.VolumeBranchesOperator;
import edu.uw.zookeeper.safari.schema.volumes.VolumeDescriptor;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

@Singleton
public class SimpleVolumeOperator {
    
    protected final LatestVolumeCache volumes;
    protected final SchemaClientService<ControlZNode<?>,?> control;
    
    @Inject
    public SimpleVolumeOperator(
            LatestVolumeCache volumes,
            SchemaClientService<ControlZNode<?>,?> control) {
        this.volumes = volumes;
        this.control = control;
    }
    
    public ListenableFuture<? extends VolumeVersion<?>> difference(
            final ZNodePath path,
            final UnsignedLong version,
            final Identifier region) throws Exception {
        return newOp(
                new Difference(region, path, version, volumes, control));
    }
    
    public ListenableFuture<? extends VolumeVersion<?>> union(
            final AssignedVolumeBranches volume,
            final UnsignedLong version) throws Exception {
        return newOp(
                new Union(
                        volume.getDescriptor().getId(), 
                        VersionedValue.valueOf(volume.getState().getVersion(), volume.getState().getValue().getBranches()), volume.getDescriptor().getPath(), version, volumes, control));
    }
    
    protected ListenableFuture<? extends VolumeVersion<?>> newOp(SimpleVolumeOperator.SimpleVolumeOp<?> op) {
        return ChainedFutures.run(
                ChainedFutures.<VolumeVersion<?>>castLast(
                    ChainedFutures.arrayList(
                            new SimpleVolumeOpChain(op),
                            2)));
    }
    
    public static abstract class SimpleVolumeOp<V extends VolumeVersion<?>> implements AsyncFunction<AssignedVolumeBranches, V> {

        protected final LatestVolumeCache volumes;
        protected final SchemaClientService<ControlZNode<?>,?> control;
        protected final ZNodePath path;
        protected final UnsignedLong version;

        protected SimpleVolumeOp(
                ZNodePath path,
                UnsignedLong version,
                LatestVolumeCache volumes,
                SchemaClientService<ControlZNode<?>,?> control) {
            this.volumes = volumes;
            this.control = control;
            this.path = path;
            this.version = version;
        }
        
        public ListenableFuture<AssignedVolumeBranches> parent() {
            ListenableFuture<AssignedVolumeBranches> future;
            try {
                future = path.isRoot() ?
                        Futures.<AssignedVolumeBranches>immediateFuture(null) : volumes.pathToVolume().apply(((AbsoluteZNodePath) path).parent());
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return future;
        }
    }

    public static class SimpleVolumeOpChain implements ChainedFutures.ChainedProcessor<ListenableFuture<? extends VolumeVersion<?>>, FutureChain<ListenableFuture<? extends VolumeVersion<?>>>> {

        protected final SimpleVolumeOperator.SimpleVolumeOp<?> op;
        
        public SimpleVolumeOpChain(
                SimpleVolumeOperator.SimpleVolumeOp<?> op) {
            this.op = op;
        }
        
        @Override
        public Optional<? extends ListenableFuture<? extends VolumeVersion<?>>> apply(
                FutureChain<ListenableFuture<? extends VolumeVersion<?>>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                return Optional.of(op.parent());
            }
            case 1:
            {
                AssignedVolumeBranches parent = (AssignedVolumeBranches) input.getLast().get(0L, TimeUnit.MILLISECONDS);
                ListenableFuture<? extends VolumeVersion<?>> future;
                try {
                    future = op.apply(parent);
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            case 2:
                return Optional.absent();
            default:
                throw new AssertionError();
            }
        }
    }
    
    public class Difference extends SimpleVolumeOperator.SimpleVolumeOp<AssignedVolumeBranches> {

        protected final Identifier region;
        
        public Difference(
                Identifier region,
                ZNodePath path,
                UnsignedLong version,
                LatestVolumeCache volumes,
                SchemaClientService<ControlZNode<?>,?> control) {
            super(path, version, volumes, control);
            this.region = region;
        }
        
        @Override
        public ListenableFuture<AssignedVolumeBranches> apply(
                final AssignedVolumeBranches parent) throws Exception {
            final ImmutableList.Builder<Records.MultiOpRequest> requests = ImmutableList.builder();
            final VolumesSchemaRequests<?> builder = VolumesSchemaRequests.create(control.materializer());
            final VolumeDescriptor volume = VolumeDescriptor.valueOf(
                    ControlZNode.hash(path, ControlSchema.Safari.Volumes.PATH, control.materializer()).asIdentifier(), path);
            requests.addAll(builder.volume(volume.getId()).create(volume.getPath()));
            final RegionAndBranches state;
            if (parent != null) {
                VolumeBranchesOperator.ParentAndChild<RegionAndBranches,RegionAndBranches> difference = AssignedVolumeOperator.create(
                        parent.getState().getValue().getRegion(),
                        VolumeBranchesOperator.create(parent.getDescriptor(), parent.getState().getValue().getBranches())).difference(
                                volume);
                state = difference.getChild();
                requests.addAll(builder.volume(parent.getDescriptor().getId()).version(version).create(Optional.of(RegionAndLeaves.copyOf(difference.getParent()))));
            } else {
                state = RegionAndBranches.empty(region);
            }
            requests.addAll(builder.volume(volume.getId()).version(version).create(Optional.of(RegionAndLeaves.copyOf(state))));
            return Futures.transform(control.materializer().submit(new IMultiRequest(requests.build())),
                    new AsyncFunction<Operation.ProtocolResponse<?>,AssignedVolumeBranches>() {
                        @Override
                        public ListenableFuture<AssignedVolumeBranches> apply(
                                Operation.ProtocolResponse<?> input)
                                throws Exception {
                            Operations.unlessMultiError((IMultiResponse) input.record());
                            return Futures.immediateFuture(
                                    AssignedVolumeBranches.valueOf(volume, VersionedValue.valueOf(version, state)));
                        }
                    });
        }
    }
    
    public class Union extends SimpleVolumeOperator.SimpleVolumeOp<EmptyVolume> {

        protected final Identifier volume;
        protected final VersionedValue<ImmutableBiMap<ZNodeName,Identifier>> branches;
        
        public Union(
                Identifier volume,
                VersionedValue<ImmutableBiMap<ZNodeName,Identifier>> branches,
                ZNodePath path,
                UnsignedLong version,
                LatestVolumeCache volumes,
                SchemaClientService<ControlZNode<?>,?> control) {
            super(path, version, volumes, control);
            this.volume = volume;
            this.branches = branches;
            checkArgument(!path.isRoot());
        }

        @Override
        public ListenableFuture<EmptyVolume> apply(
                final AssignedVolumeBranches parent)
                throws Exception {
            final VolumesSchemaRequests<?> builder = VolumesSchemaRequests.create(control.materializer());
            final ImmutableList.Builder<Records.MultiOpRequest> requests = ImmutableList.builder();
            final RegionAndBranches union = AssignedVolumeOperator.create(
                    parent.getState().getValue().getRegion(),
                    VolumeBranchesOperator.create(
                            parent.getDescriptor(), parent.getState().getValue().getBranches()))
                            .union(VolumeDescriptor.valueOf(volume, path),
                                    branches.getValue());
            requests.addAll(builder.volume(parent.getDescriptor().getId()).version(version).create(Optional.of(RegionAndLeaves.copyOf(union))));
            requests.addAll(builder.volume(volume).version(version).create(Optional.<RegionAndLeaves>absent()));
            return Futures.transform(
                    control.materializer().submit(new IMultiRequest(requests.build())),
                    new AsyncFunction<Operation.ProtocolResponse<?>,EmptyVolume>() {
                        @Override
                        public ListenableFuture<EmptyVolume> apply(
                                Operation.ProtocolResponse<?> input)
                                throws Exception {
                            Operations.unlessMultiError((IMultiResponse) input.record());
                            return Futures.immediateFuture(
                                    EmptyVolume.valueOf(VolumeDescriptor.valueOf(volume, path), version));
                        }
                    });
        }
    }
}