package edu.uw.zookeeper.safari.storage.volumes;

import java.util.List;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class CreateVolume<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<List<O>, Boolean> {
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Boolean> call(
            Identifier id,
            UnsignedLong version,
            Materializer<StorageZNode<?>,O> materializer) {
        final ZNodePath path = StorageSchema.Safari.Volumes.Volume.pathOf(id);
        final ZNodePath rootPath = path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL);
        final ZNodePath logPath = path.join(StorageSchema.Safari.Volumes.Volume.Log.LABEL);
        final ZNodePath versionPath = logPath.join(ZNodeLabel.fromString(version.toString()));
        ImmutableList.Builder<ListenableFuture<O>> futures = ImmutableList.builder();
        for (ZNodePath p: ImmutableList.of(
                path, 
                rootPath,
                logPath,
                versionPath)) {
            futures.add(materializer.create(p).call());
        }
        return Futures.transform(
                Futures.allAsList(futures.build()), 
                new CreateVolume<O>(versionPath, materializer));
    }
    
    protected final ZNodePath path;
    protected final Materializer<StorageZNode<?>,O> materializer;
    
    protected CreateVolume(
            ZNodePath path,
            Materializer<StorageZNode<?>,O> materializer) {
        this.path = path;
        this.materializer = materializer;
    }

    @Override
    public ListenableFuture<Boolean> apply(List<O> input)
            throws Exception {
        Optional<Operation.Error> error = null;
        for (O response: input) {
            error = Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS, KeeperException.Code.NONODE);
        }
        return Futures.immediateFuture(Boolean.valueOf(!error.isPresent()));
    }
}