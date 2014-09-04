package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class LeaderProposal<O extends Operation.ProtocolResponse<?>> implements Callable<ListenableFuture<LeaderEpoch>>, AsyncFunction<List<O>,LeaderEpoch> {

    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<LeaderEpoch> call(
            Identifier region, 
            Identifier peer, 
            Optional<Integer> epoch,
            Materializer<ControlZNode<?>,O> materializer) {
        return create(region, peer, epoch, materializer).call();
    }

    public static <O extends Operation.ProtocolResponse<?>> LeaderProposal<O> create(
            Identifier region, 
            Identifier peer, 
            Optional<Integer> epoch,
            Materializer<ControlZNode<?>,O> materializer) {
        return new LeaderProposal<O>(region, peer, epoch, materializer);
    }
    
    private final Logger logger;
    private final AbsoluteZNodePath path;
    private final Identifier peer;
    private final Identifier region;
    private final Optional<Integer> epoch;
    private final Materializer<ControlZNode<?>,O> materializer;
    
    protected LeaderProposal(
            Identifier region, 
            Identifier peer, 
            Optional<Integer> epoch,
            Materializer<ControlZNode<?>,O> materializer) {
        this.logger = LogManager.getLogger(this);
        this.materializer = materializer;
        this.epoch = epoch;
        this.peer = peer;
        this.region = region;
        this.path = ControlSchema.Safari.Regions.Region.Leader.pathOf(region);
    }
    
    @Override
    public ListenableFuture<LeaderEpoch> call() {
        ImmutableList.Builder<ListenableFuture<O>> futures = ImmutableList.builder();
        if (epoch.isPresent()) {
            logger.info("Proposing leader {} for region {} following epoch {}", peer, region, epoch.get());
            Materializer<ControlZNode<?>,O>.Operator<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Identifier>> request = materializer.<Identifier>setData(path, peer);
            request.get().delegate().setVersion(epoch.get());
            futures.add(request.call());
        } else {
            logger.info("Proposing new leader {} for region {}", peer, region);
            futures.add(materializer.create(path, peer).call());
        }
        // create2 isn't in the stable release
        // so we'll have to issue multiple requests to get the version anyway
        futures.add(materializer.sync(path).call()).add(materializer.getData(path).call());
        return Futures.transform(Futures.allAsList(futures.build()), this, SameThreadExecutor.getInstance());
    }

    @Override
    public ListenableFuture<LeaderEpoch> apply(List<O> input) throws Exception {
        Optional<LeaderEpoch> result = Optional.absent();
        for (O response: input) {
            Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS, KeeperException.Code.BADVERSION);
        }
        materializer.cache().lock().readLock().lock();
        try {
            ControlSchema.Safari.Regions.Region.Leader node = (ControlSchema.Safari.Regions.Region.Leader) materializer.cache().cache().get(path);
            if (node != null) {
                result = node.getLeaderEpoch();
            }
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
        if (result.isPresent()) {
            return Futures.immediateFuture(result.get());
        } else {
            // shouldn't get here, but let's just try again
            return call();
        }
    }
}
