package edu.uw.zookeeper.safari.region;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.volume.AssignedVolumeOperator;
import edu.uw.zookeeper.safari.volume.RegionAndBranches;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;
import edu.uw.zookeeper.safari.volume.AssignParameters;
import edu.uw.zookeeper.safari.volume.VolumeBranchesOperator;
import edu.uw.zookeeper.safari.volume.VolumeDescriptor;
import edu.uw.zookeeper.safari.volume.MergeParameters;
import edu.uw.zookeeper.safari.volume.VolumeOperation;
import edu.uw.zookeeper.safari.volume.VolumeOperatorParameters;
import edu.uw.zookeeper.safari.volume.SplitParameters;

public class VolumeOperationRequests<O extends Operation.ProtocolResponse<?>> implements Function<VolumeOperation<?>, List<Records.MultiOpRequest>> {

    public static <O extends Operation.ProtocolResponse<?>> VolumeOperationRequests<O> create(
            VolumesSchemaRequests<O> schema,
            Function<? super Identifier, ZNodePath> paths,
            Function<? super VersionedId, Optional<RegionAndBranches>> states) {
        return new VolumeOperationRequests<O>(schema, paths, states);
    }
    
    protected final VolumesSchemaRequests<O> schema;
    protected final Function<? super Identifier, ZNodePath> paths;
    protected final Function<? super VersionedId, Optional<RegionAndBranches>> states;
    
    protected VolumeOperationRequests(
            VolumesSchemaRequests<O> schema,
            Function<? super Identifier, ZNodePath> paths,
            Function<? super VersionedId, Optional<RegionAndBranches>> states) {
        super();
        this.schema = schema;
        this.paths = paths;
        this.states = states;
    }
    
    public VolumesSchemaRequests<O> schema() {
        return schema;
    }
    
    public Function<? super Identifier, ZNodePath> paths() {
        return paths;
    }
    
    public Function<? super VersionedId, Optional<RegionAndBranches>> states() {
        return states;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Records.MultiOpRequest> apply(VolumeOperation<?> input) {
        AbstractVolumeOperationRequests<O,?> operation;
        switch (input.getOperator().getOperator()) {
        case MERGE:
            operation = new MergeOperationRequests<O>(schema, paths, states, input.getVolume());
            break;
        case SPLIT:
            operation = new SplitOperationRequests<O>(schema, paths, states, input.getVolume());
            break;
        case TRANSFER:
            operation = new TransferOperationRequests<O>(schema, paths, states, input.getVolume());
            break;
        default:
            throw new AssertionError();
        }
        return ((AbstractVolumeOperationRequests<O,VolumeOperatorParameters>) operation).apply((VolumeOperatorParameters) input.getOperator().getParameters());
    }

    protected abstract static class AbstractVolumeOperationRequests<O extends Operation.ProtocolResponse<?>,T extends VolumeOperatorParameters> implements Function<T, List<Records.MultiOpRequest>> {

        protected final VolumesSchemaRequests<O> schema;
        protected final Function<? super Identifier, ZNodePath> paths;
        protected final Function<? super VersionedId, Optional<RegionAndBranches>> states;
        protected final VersionedId volume;
        
        protected AbstractVolumeOperationRequests(
                VolumesSchemaRequests<O> schema,
                Function<? super Identifier, ZNodePath> paths,
                Function<? super VersionedId, Optional<RegionAndBranches>> states,
                VersionedId volume) {
            this.schema = schema;
            this.paths = paths;
            this.states = states;
            this.volume = volume;
        }
        
        public final VersionedId volume() {
            return volume;
        }

        public final VolumesSchemaRequests<O> schema() {
            return schema;
        }
        
        public final Function<? super Identifier, ZNodePath> paths() {
            return paths;
        }
        
        public final Function<? super VersionedId, Optional<RegionAndBranches>> states() {
            return states;
        }
        
        protected AssignedVolumeOperator operator() {
            RegionAndBranches state = states().apply(volume()).get();
            return AssignedVolumeOperator.create(
                    state.getRegion(), 
                    VolumeBranchesOperator.create(
                            VolumeDescriptor.valueOf(volume().getValue(), 
                                    paths().apply(volume().getValue())), 
                            state.getBranches()));
        }
    }
    
    protected final static class TransferOperationRequests<O extends Operation.ProtocolResponse<?>> extends AbstractVolumeOperationRequests<O,AssignParameters> {

        public TransferOperationRequests(
                VolumesSchemaRequests<O> schema,
                Function<? super Identifier, ZNodePath> paths,
                Function<? super VersionedId, Optional<RegionAndBranches>> states,
                VersionedId volume) {
            super(schema, paths, states, volume);
        }
        
        @Override
        public List<Records.MultiOpRequest> apply(AssignParameters input) {
            final RegionAndBranches assigned = operator().assign(
                    input.getRegion());
            return schema().volume(volume().getValue())
                    .version(input.getVersion())
                    .create(Optional.of(RegionAndLeaves.copyOf(assigned)));
        }
    }

    protected final static class MergeOperationRequests<O extends Operation.ProtocolResponse<?>> extends AbstractVolumeOperationRequests<O,MergeParameters> {

        public MergeOperationRequests(
                VolumesSchemaRequests<O> schema,
                Function<? super Identifier, ZNodePath> paths,
                Function<? super VersionedId, Optional<RegionAndBranches>> states,
                VersionedId volume) {
            super(schema, paths, states, volume);
        }
        
        @Override
        public List<Records.MultiOpRequest> apply(MergeParameters input) {
            final RegionAndBranches union = operator().union(
                    VolumeDescriptor.valueOf(
                            input.getParent().getValue(), 
                            paths.apply(input.getParent().getValue())), 
                    states.apply(input.getParent()).get().getBranches());
            return ImmutableList.<Records.MultiOpRequest>builder()
                    .addAll(schema.volume(volume().getValue())
                            .version(input.getVersion())
                            .create(Optional.<RegionAndLeaves>absent()))
                    .addAll(schema.volume(input.getParent().getValue())
                            .version(input.getVersion())
                            .create(Optional.of(RegionAndLeaves.copyOf(union))))
                    .build();
        }
    }

    protected final static class SplitOperationRequests<O extends Operation.ProtocolResponse<?>> extends AbstractVolumeOperationRequests<O,SplitParameters> {

        public SplitOperationRequests(
                VolumesSchemaRequests<O> schema,
                Function<? super Identifier, ZNodePath> paths,
                Function<? super VersionedId, Optional<RegionAndBranches>> states,
                VersionedId volume) {
            super(schema, paths, states, volume);
        }
        
        @Override
        public List<Records.MultiOpRequest> apply(
                final SplitParameters input) {
            final ImmutableList.Builder<Records.MultiOpRequest> result = 
                    ImmutableList.builder();
            final VolumeDescriptor child = 
                    VolumeDescriptor.valueOf(
                            input.getLeaf(), 
                            paths().apply(volume().getValue()).join(input.getBranch()));
            final VolumeBranchesOperator.ParentAndChild<RegionAndBranches,RegionAndBranches> difference = 
                    operator().difference(child);
            result.addAll(schema.volume(volume().getValue()).version(input.getVersion())
                        .create(Optional.<RegionAndLeaves>of(RegionAndLeaves.copyOf(difference.getParent()))));
            result.addAll(
                    new TransferOperationRequests<O>(
                            schema,
                            new Function<Identifier, ZNodePath>() {
                                @Override
                                public ZNodePath apply(Identifier volume) {
                                    if (volume.equals(child.getId())) {
                                        return child.getPath();
                                    } else {
                                        return paths().apply(volume);
                                    }
                                }
                            },
                            new Function<VersionedId, Optional<RegionAndBranches>>() {
                                @Override
                                public Optional<RegionAndBranches> apply(VersionedId volume) {
                                    if (VersionedId.valueOf(input.getVersion(), input.getLeaf()).equals(volume)) {
                                        return Optional.of(difference.getChild());
                                    } else {
                                        return states().apply(volume);
                                    }
                                }
                            },
                            VersionedId.valueOf(input.getVersion(), 
                                    input.getLeaf())).apply(input));
            return result.build();
        }
    }
}
