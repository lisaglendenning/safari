package edu.uw.zookeeper.safari.control.volumes;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;

public final class VolumeOperationCoordinatorEntry extends SimpleToStringListenableFuture<VolumeLogEntryPath> {

    public static VolumeOperationCoordinatorEntry newEntry(
            VolumeOperation<?> operation,
            Materializer<ControlZNode<?>,?> materializer) {
        ListenableFuture<VolumeLogEntryPath> future = 
                Futures.transform(
                    materializer.submit(
                        VolumesSchemaRequests.create(materializer)
                        .version(operation.getVolume())
                        .logOperator(operation.getOperator())),
                    new ResponseToPath(operation.getVolume()),
                    SameThreadExecutor.getInstance());
        return create(operation, future);
    }

    public static VolumeOperationCoordinatorEntry existingEntry(
            VolumeOperation<?> operation,
            VolumeLogEntryPath path) {
        return create(operation, Futures.immediateFuture(path));
    }

    public static VolumeOperationCoordinatorEntry create(
            VolumeOperation<?> operation,
            ListenableFuture<VolumeLogEntryPath> future) {
        return new VolumeOperationCoordinatorEntry(operation, future);
    }
    
    private final VolumeOperation<?> operation;
    
    protected VolumeOperationCoordinatorEntry(
            VolumeOperation<?> operation,
            ListenableFuture<VolumeLogEntryPath> future) {
        super(future);
        this.operation = operation;
    }
    
    public VolumeOperation<?> operation() {
        return operation;
    }
    
    @Override
    protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper helper) {
        return super.toStringHelper(helper.addValue(operation));
    }

    protected static final class ResponseToPath implements AsyncFunction<Operation.ProtocolResponse<?>, VolumeLogEntryPath> {
        
        private final VersionedId volume;
        
        public ResponseToPath(VersionedId volume) {
            this.volume = volume;
        }

        @Override
        public ListenableFuture<VolumeLogEntryPath> apply(Operation.ProtocolResponse<?> input) throws Exception {
            ZNodeLabel label = AbsoluteZNodePath.fromString(((Records.PathGetter) Operations.unlessError(input.record())).getPath()).label();
            return Futures.immediateFuture(VolumeLogEntryPath.valueOf(volume, Sequential.fromString(label.toString())));
        }
    }
}