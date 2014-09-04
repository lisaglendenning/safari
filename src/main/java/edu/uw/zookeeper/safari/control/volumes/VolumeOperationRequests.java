package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.VersionedValue;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.schema.volumes.AssignParameters;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeOperator;
import edu.uw.zookeeper.safari.schema.volumes.BoundVolumeOperator;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndBranches;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeBranchesOperator;
import edu.uw.zookeeper.safari.schema.volumes.VolumeDescriptor;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperatorParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

public final class VolumeOperationRequests<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<VolumeOperation<?>, List<Records.MultiOpRequest>> {

    public static <O extends Operation.ProtocolResponse<?>> VolumeOperationRequests<O> create(
            VolumesSchemaRequests<O> schema,
            AsyncFunction<? super VersionedId, VolumeVersion<?>> states) {
        return new VolumeOperationRequests<O>(schema, states);
    }
    
    private final VolumesSchemaRequests<O> schema;
    private final AsyncFunction<? super VersionedId, VolumeVersion<?>> states;
    
    protected VolumeOperationRequests(
            VolumesSchemaRequests<O> schema,
            AsyncFunction<? super VersionedId, VolumeVersion<?>> states) {
        super();
        this.schema = schema;
        this.states = states;
    }
    
    public VolumesSchemaRequests<O> schema() {
        return schema;
    }
    
    public AsyncFunction<? super VersionedId, VolumeVersion<?>> states() {
        return states;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ListenableFuture<List<Records.MultiOpRequest>> apply(VolumeOperation<?> input) throws Exception {
        AbstractVolumeOperationRequests<?> operation;
        switch (input.getOperator().getOperator()) {
        case MERGE:
            operation = new MergeOperationRequests((VolumeOperation<MergeParameters>) input);
            break;
        case SPLIT:
            operation = new SplitOperationRequests((VolumeOperation<SplitParameters>) input);
            break;
        case TRANSFER:
            operation = new TransferOperationRequests((VolumeOperation<? extends AssignParameters>) input);
            break;
        default:
            throw new AssertionError();
        }
        CallablePromiseTask<? extends AbstractVolumeOperationRequests<?>,List<Records.MultiOpRequest>> task = CallablePromiseTask.create(
                operation, 
                SettableFuturePromise.<List<Records.MultiOpRequest>>create());
        task.task().addListener(task, SameThreadExecutor.getInstance());
        return task;
    }
    
    protected static final Function<VolumeVersion<?>, AssignedVolumeOperator> TO_OPERATOR = new Function<VolumeVersion<?>, AssignedVolumeOperator>() {
        @Override
        public AssignedVolumeOperator apply(VolumeVersion<?> input) {
            AssignedVolumeBranches state = (AssignedVolumeBranches) input;
            return AssignedVolumeOperator.create(
                    state.getState().getValue().getRegion(), 
                    VolumeBranchesOperator.create(
                            state.getDescriptor(), 
                            state.getState().getValue().getBranches()));
        }
    };

    protected abstract class AbstractVolumeOperationRequests<T extends VolumeOperatorParameters> extends ToStringListenableFuture<List<Object>> implements Callable<Optional<List<Records.MultiOpRequest>>> {

        protected final VolumeOperation<? extends T> operation;
        protected final ListenableFuture<AssignedVolumeOperator> operator;
        
        protected AbstractVolumeOperationRequests(
                VolumeOperation<? extends T> operation) {
            this.operation = operation;        
            ListenableFuture<AssignedVolumeOperator> operator;
            try {
                operator = Futures.transform(
                        states().apply(operation.getVolume()), 
                        TO_OPERATOR,
                        SameThreadExecutor.getInstance());
            } catch (Exception e) {
                operator = Futures.immediateFailedFuture(e);
            }
            this.operator = operator;
        }
        
        public final VolumeOperation<? extends T> operation() {
            return operation;
        }
        
        protected ListenableFuture<AssignedVolumeOperator> operator() {
            return operator;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected ListenableFuture<List<Object>> delegate() {
            return Futures.<Object>allAsList(operator());
        }
    }
    
    protected final class TransferOperationRequests extends AbstractVolumeOperationRequests<AssignParameters> {

        public TransferOperationRequests(
                VolumeOperation<? extends AssignParameters> operation) {
            super(operation);
        }
        
        @Override
        public Optional<List<Records.MultiOpRequest>> call() throws Exception {
            if (operator().isDone()) {
                AssignedVolumeOperator operator = operator().get();
                final RegionAndBranches assigned = operator.assign(
                        operation().getOperator().getParameters().getRegion());
                return Optional.<List<Records.MultiOpRequest>>of(
                        schema().volume(operation().getVolume().getValue())
                        .version(operation().getOperator().getParameters().getVersion())
                        .create(Optional.of(RegionAndLeaves.copyOf(assigned))));
            }
            return Optional.absent();
        }
    }

    protected final class MergeOperationRequests extends AbstractVolumeOperationRequests<MergeParameters> {

        private final ListenableFuture<? extends VolumeVersion<?>> parent;
        
        public MergeOperationRequests(
                VolumeOperation<MergeParameters> operation) {
            super(operation);
            ListenableFuture<? extends VolumeVersion<?>> parent;
            try {
                parent = states.apply(operation.getOperator().getParameters().getParent());
            } catch (Exception e) {
                parent = Futures.immediateFailedFuture(e);
            }
            this.parent = parent;
        }
        
        public ListenableFuture<? extends VolumeVersion<?>> parent() {
            return parent;
        }
        
        @Override
        public Optional<List<Records.MultiOpRequest>> call() throws Exception {
            if (operator().isDone()) {
                AssignedVolumeOperator operator = operator().get();
                if (parent().isDone()) {
                    AssignedVolumeBranches parent = (AssignedVolumeBranches) parent().get();
                    final RegionAndBranches union = operator.union(
                            parent.getDescriptor(), 
                            parent.getState().getValue().getBranches());
                    return Optional.<List<Records.MultiOpRequest>>of(
                            ImmutableList.<Records.MultiOpRequest>builder()
                            .addAll(schema
                                    .volume(operation().getVolume().getValue())
                                    .version(operation().getOperator().getParameters().getVersion())
                                    .create(Optional.<RegionAndLeaves>absent()))
                            .addAll(schema
                                    .volume(operation().getOperator().getParameters().getParent().getValue())
                                    .version(operation().getOperator().getParameters().getVersion())
                                    .create(Optional.of(RegionAndLeaves.copyOf(union))))
                            .build());
                }
            }
            return Optional.absent();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected ListenableFuture<List<Object>> delegate() {
            return Futures.<Object>allAsList(operator(), parent());
        }
    }

    protected final class SplitOperationRequests extends AbstractVolumeOperationRequests<SplitParameters> {

        public SplitOperationRequests(
                VolumeOperation<SplitParameters> operation) {
            super(operation);
        }
        
        @Override
        public Optional<List<Records.MultiOpRequest>> call() throws Exception {
            if (operator().isDone()) {
                AssignedVolumeOperator operator = operator().get();
                final ImmutableList.Builder<Records.MultiOpRequest> requests = 
                        ImmutableList.builder();
                final VolumeDescriptor child = 
                        VolumeDescriptor.valueOf(
                                operation().getOperator().getParameters().getLeaf(), 
                                operator.operator().volume().getPath().join(operation().getOperator().getParameters().getBranch()));
                final VolumeBranchesOperator.ParentAndChild<RegionAndBranches,RegionAndBranches> difference = 
                        operator.difference(child);
                requests.addAll(schema
                            .volume(operation.getVolume().getValue())
                            .version(operation.getOperator().getParameters().getVersion())
                            .create(Optional.<RegionAndLeaves>of(
                                    RegionAndLeaves.copyOf(difference.getParent()))));
                final VolumeOperationRequests<O> derived = VolumeOperationRequests.create(
                        schema(),
                        new AsyncFunction<VersionedId, VolumeVersion<?>>() {
                            @Override
                            public ListenableFuture<VolumeVersion<?>> apply(VersionedId input) throws Exception {
                                if (input.getValue().equals(operation.getOperator().getParameters().getLeaf()) && input.getVersion().equals(operation.getOperator().getParameters().getVersion())) {
                                    return Futures.<VolumeVersion<?>>immediateFuture(
                                            AssignedVolumeBranches.valueOf(
                                                    child, 
                                                    VersionedValue.valueOf(
                                                            input.getVersion(),
                                                            difference.getChild())));
                                } else {
                                    return states().apply(input);
                                }
                            }
                        });
                requests.addAll(derived.apply(
                        VolumeOperation.valueOf(
                                VersionedId.valueOf(
                                        operation.getOperator().getParameters().getVersion(), 
                                        operation.getOperator().getParameters().getLeaf()), 
                                BoundVolumeOperator.valueOf(
                                        VolumeOperator.TRANSFER, 
                                        operation.getOperator().getParameters())))
                        .get());
                return Optional.<List<Records.MultiOpRequest>>of(requests.build());
            }
            return Optional.absent();
        }
    }
}
