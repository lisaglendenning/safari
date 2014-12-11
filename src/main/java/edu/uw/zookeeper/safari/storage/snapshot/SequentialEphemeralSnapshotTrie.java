package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.ListenableFutureActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Request;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Builds a label trie rooted at the destination snapshot prefix such that the
 * leaves are all the sequential ephemerals in the snapshot, children nodes
 * are sorted by sequence, and the value stored at each leaf is the 
 * corresponding path of the ephemeral snapshot.
 */
public final class SequentialEphemeralSnapshotTrie<O extends Operation.ProtocolResponse<?>> extends ListenableFutureActor<ZNodePath, List<O>, SubmittedRequests<? extends Records.Request, O>> implements Callable<ListenableFuture<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>>> {

    public static <O extends Operation.ProtocolResponse<?>> Callable<ListenableFuture<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>>> create(
            ZNodePath snapshot,
            Materializer<StorageZNode<?>,O> materializer,
            Logger logger) {
        return create(
                snapshot, 
                materializer.cache(),
                materializer,
                logger);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> Callable<ListenableFuture<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>>> create(
            ZNodePath snapshot,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            final TaskExecutor<? super Records.Request,O> client,
            Logger logger) {
        return new SequentialEphemeralSnapshotTrie<O>(
                snapshot, 
                cache,
                client,
                SettableFuturePromise.<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>>create(), 
                logger);
    }

    private final ZNodePath snapshot;
    private final SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> trie;
    private final ImmutableSet.Builder<AbsoluteZNodePath> leaves;
    private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
    private final Function<ZNodePath, ? extends SubmittedRequests<? extends Records.Request,O>> getSessionValues;
    private final Promise<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>> promise;
    private boolean isReady;
    
    @SuppressWarnings("unchecked")
    protected SequentialEphemeralSnapshotTrie(
            ZNodePath snapshot,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            final TaskExecutor<? super Records.Request,O> client,
            Promise<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>> promise,
            Logger logger) {
        super(Queues.<SubmittedRequests<? extends Request, O>>newConcurrentLinkedQueue(), logger);
        this.cache = cache;
        this.snapshot = snapshot;
        this.getSessionValues = new Function<ZNodePath, SubmittedRequests<? extends Records.Request,O>>() {
            final Function<ZNodePath, List<Records.Request>> requests = 
                    Functions.compose(
                            PathToRequests.forRequests(
                                Operations.Requests.sync(), 
                                Operations.Requests.getChildren()),
                            JoinToPath.forName(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.LABEL));
            @Override
            public SubmittedRequests<? extends Records.Request, O> apply(ZNodePath input) {
                return SubmittedRequests.submit(client, requests.apply(input));
            }
        };
        this.trie = SimpleLabelTrie.forRoot(SequentialNode.<AbsoluteZNodePath>sequentialRoot());
        this.leaves = ImmutableSet.builder();
        this.promise = promise;
        this.isReady = false;
        
        promise.addListener(this, MoreExecutors.directExecutor());
    }

    /**
     * 
     * Assumes all Ephemerals.Sessions.Session are already cached.
     * 
     * Threadsafe. Starts running on first call, and any subsequent
     * calls will simply return the same future.
     * 
     * @return trie of sequential ephemerals and the set of leaves in the trie
     */
    @Override
    public ListenableFuture<Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>> call() {
        cache.lock().readLock().lock();
        try {
            synchronized (this) {
                if (!promise.isDone() && !isReady) {
                    try {
                        StorageZNode<?> node = cache.cache().get(snapshot);
                        for (ZNodeLabel label: ImmutableList.of(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL,
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.LABEL)) {
                            if (node != null) {
                                node = node.get(label);
                            }
                        }
                        if (node != null) {
                            for (StorageZNode<?> session: node.values()) {
                                submit(session.path());
                            }
                        }
                        isReady = true;
                        run();
                    } catch (Exception e) {
                        promise.setException(e);
                    }
                }
            }
        } finally {
            cache.lock().readLock().unlock();
        }
        return promise;
    }

    @Override
    public synchronized boolean isReady() {
        return isReady && super.isReady();
    }
    
    @Override
    public ListenableFuture<List<O>> submit(ZNodePath session) {
        SubmittedRequests<? extends Records.Request, O> task = getSessionValues.apply(session);
        if (!send(task)) {
            task.cancel(false);
            throw new RejectedExecutionException();
        }
        return task;
    }
    
    @Override
    public synchronized void run() {
        super.run();
        if (promise.isDone()) {
            stop();
        } else if (isReady && mailbox.isEmpty()) {
            promise.set(Pair.create(trie, leaves.build()));
        }
    }

    @Override
    protected void doRun() {
        try {
            super.doRun();
        } catch (Exception e) {
            promise.setException(e);
        }
    }
    
    @Override
    protected boolean doApply(SubmittedRequests<? extends Request, O> input)
            throws Exception {
        final int last = input.getValue().size()-1;
        final Records.PathGetter request = (Records.PathGetter) input.getValue().get(last);
        final Records.Response response = input.get().get(last).record();
        if (!Operations.maybeError(request, response, KeeperException.Code.NONODE).isPresent()) {
            final ZNodePath path = ZNodePath.fromString(request.getPath());
            cache.lock().readLock().lock();
            try {
                final StorageZNode<?> values = cache.cache().get(path);
                assert (values.size() == ((Records.ChildrenGetter) response).getChildren().size());
                for (StorageZNode<?> child: values.values()) {
                    final StorageZNode.EscapedNamedZNode<?> ephemeral = (StorageZNode.EscapedNamedZNode<?>) child;
                    final int index = ephemeral.name().lastIndexOf(ZNodeName.SLASH);
                    assert ((index < 0) || ((index < ephemeral.name().length() - 1) && (index > 0)));
                    final String label = (index < 0) ? ephemeral.name() : ephemeral.name().substring(index+1);
                    final Optional<? extends Sequential<String, ?>> sequential = Sequential.maybeFromString(label);
                    if (sequential.isPresent()) {
                        AbsoluteZNodePath triePath = (AbsoluteZNodePath) ZNodePath.root().join(ZNodeName.fromString(ephemeral.name()));
                        SequentialNode<AbsoluteZNodePath> node = SequentialNode.putIfAbsent(trie, triePath, (AbsoluteZNodePath) ephemeral.path(), sequential.get());
                        leaves.add((AbsoluteZNodePath) node.path());
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        return true;
    }
}
