package edu.uw.zookeeper.safari.peer;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;

public final class LeaderProposal extends RunnablePromiseTask<Pair<AbsoluteZNodePath,Identifier>, LeaderEpoch> {

    public static LeaderProposal newInstance(
            Identifier region, 
            Identifier peer, 
            Optional<Integer> epoch,
            Materializer<ControlZNode<?>,?> materializer) {
        return new LeaderProposal(region, peer, epoch, materializer, SettableFuturePromise.<LeaderEpoch>create());
    }

    private final Optional<Integer> epoch;
    private final Materializer<ControlZNode<?>,?> materializer;
    private ListenableFuture<? extends Operation.ProtocolResponse<?>> request;
    
    public LeaderProposal(
            Identifier region, 
            Identifier peer, 
            Optional<Integer> epoch,
            Materializer<ControlZNode<?>,?> materializer,
            Promise<LeaderEpoch> delegate) {
        super(Pair.create(ControlSchema.Safari.Regions.Region.Leader.pathOf(region), peer), delegate);
        this.materializer = materializer;
        this.epoch = epoch;
        this.request = null;
        
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public synchronized void run() {
        super.run();
        
        if (isDone()) {
            if (isCancelled()) {
                if (request != null) {
                    request.cancel(false);
                }
            }
        }
    }
    
    @Override
    public Optional<LeaderEpoch> call() throws Exception {
        Optional<LeaderEpoch> result = Optional.absent();
        if (request == null) {
            // create2 isn't in the stable release
            // so we'll have to issue multiple requests to get the version anyway
            if (epoch.isPresent()) {
                Materializer<ControlZNode<?>,?>.Operator<Operations.Requests.SerializedData<ISetDataRequest, Operations.Requests.SetData, Identifier>> request = materializer.<Identifier>setData(task().first(), task().second());
                request.get().delegate().setVersion(epoch.get());
                request.call();
            } else {
                materializer.create(task().first(), task().second()).call();
            }
            materializer.sync(task().first());
            request = materializer.getData(task().first()).call();
            request.addListener(this, SameThreadExecutor.getInstance());
        } else if (request.isDone()) {
            try {
                Operations.unlessError(
                        request.get().record(),
                        String.valueOf(request.get()));
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
                request = null;
                result = call();
            }
        }
        return result;
    }
}