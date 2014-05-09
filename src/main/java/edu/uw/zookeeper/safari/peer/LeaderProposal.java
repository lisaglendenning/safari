package edu.uw.zookeeper.safari.peer;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;

public final class LeaderProposal extends PromiseTask<Pair<AbsoluteZNodePath,Identifier>, LeaderEpoch> implements Runnable, Callable<Optional<LeaderEpoch>> {

    public static LeaderProposal create(
            Identifier region, 
            Identifier peer, 
            Optional<Integer> epoch,
            Materializer<ControlZNode<?>,?> materializer,
            Promise<LeaderEpoch> promise) {
        LeaderProposal instance = new LeaderProposal(region, peer, epoch, materializer, promise);
        instance.run();
        return instance;
    }

    private final CallablePromiseTask<LeaderProposal,LeaderEpoch> runnable;
    private final Optional<Integer> epoch;
    private final Materializer<ControlZNode<?>,?> materializer;
    private Optional<ListenableFuture<List<Operation.ProtocolResponse<?>>>> request;
    
    public LeaderProposal(
            Identifier region, 
            Identifier peer, 
            Optional<Integer> epoch,
            Materializer<ControlZNode<?>,?> materializer,
            Promise<LeaderEpoch> delegate) {
        super(Pair.create(ControlSchema.Safari.Regions.Region.Leader.pathOf(region), peer), delegate);
        this.materializer = materializer;
        this.epoch = epoch;
        this.request = Optional.absent();
        this.runnable = CallablePromiseTask.create(this, this);
        
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public synchronized void run() {
        if (isDone()) {
            if (isCancelled()) {
                if (request.isPresent()) {
                    request.get().cancel(false);
                }
            }
        } else {
            runnable.run();
        }
    }
    
    @Override
    public Optional<LeaderEpoch> call() throws Exception {
        if (!request.isPresent()) {
            // create2 isn't in the stable release
            // so we'll have to issue multiple requests to get the version anyway
            ImmutableList.Builder<ListenableFuture<? extends Operation.ProtocolResponse<?>>> futures = ImmutableList.builder();
            if (epoch.isPresent()) {
                Materializer<ControlZNode<?>,?>.Operator<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Identifier>> request = materializer.<Identifier>setData(task().first(), task().second());
                request.get().delegate().setVersion(epoch.get());
                futures.add(request.call());
            } else {
                futures.add(materializer.create(task().first(), task().second()).call());
            }
            futures.add(materializer.sync(task().first()).call()).add(materializer.getData(task().first()).call());
            request = Optional.of(Futures.allAsList(futures.build()));
            request.get().addListener(this, SameThreadExecutor.getInstance());
        } else if (request.get().isDone()) {
            Optional<LeaderEpoch> result = Optional.absent();
            try {
                for (Operation.ProtocolResponse<?> response: request.get().get()) {
                    Operations.unlessError(response.record(),
                            String.valueOf(request.get()));
                }
                materializer.cache().lock().readLock().lock();
                try {
                    ControlZNode<?> node = materializer.cache().cache().get(task().first());
                    result = Optional.fromNullable(
                                    LeaderEpoch.fromMaterializer().apply(node));
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
            } catch (Exception e) {}
            
            // shouldn't get here, but let's just try again
            if (! result.isPresent()) {
                request = Optional.absent();
                return call();
            } else {
                return result;
            }
        }
        return Optional.absent();
    }
}