package edu.uw.zookeeper.safari.storage.snapshot;

import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.FutureChain.FutureListChain;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.SchemaElementLookup;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ICreateRequest;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Request;
import edu.uw.zookeeper.protocol.proto.Stats;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.snapshot.SequentialEphemeralIterator.CommittedChildIterator;
import edu.uw.zookeeper.safari.storage.snapshot.SequentialEphemeralTrieBuilder.SequentialNode;

public final class RecreateEphemerals<O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> & Connection.Listener<? super Operation.Response>> extends RecreateSessionValues<T,StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral> {

    public static <O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> & Connection.Listener<? super Operation.Response>> ListenableFuture<ZNodePath> listen(
            ZNodePath snapshot,
            SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> sequentialTrie,
            Set<AbsoluteZNodePath> sequentials,
            Function<? super Long, ? extends T> executors,
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        SequentialEphemeralIterator iterator = SequentialEphemeralIterator.create(
                snapshot.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL), 
                sequentialTrie, 
                sequentials, 
                materializer, 
                cacheEvents, 
                service, 
                logger); 
        return listen(snapshot, sequentialTrie, iterator.iterators(), ImmutableList.of(iterator), executors, materializer, cacheEvents, service, logger);
    }
    
    protected static <O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> & Connection.Listener<? super Operation.Response>> ListenableFuture<ZNodePath> listen(
            ZNodePath snapshot,
            SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> sequentials,
            Map<ZNodePath, CommittedChildIterator> iterators,
            ImmutableList<? extends WatchMatchServiceListener> listeners,
            Function<? super Long, ? extends T> executors,
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        final FutureCallback<?> callback = Watchers.StopServiceOnFailure.create(service);
        final RecreateEphemerals<O,T> instance = create(snapshot, materializer.schema(), sequentials, iterators, callback, executors, materializer);
        final Promise<ZNodePath> committed = committed(instance, materializer.cache(), cacheEvents, logger);
        listeners = ImmutableList.<WatchMatchServiceListener>builder()
                .addAll(listeners(instance, materializer, cacheEvents, service, logger))
                .addAll(listeners).build();
        listen(listeners, committed, service);
        return committed;
    }
    
    @SuppressWarnings("unchecked")
    protected static <O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> & Connection.Listener<? super Operation.Response>> RecreateEphemerals<O,T> create(
            ZNodePath snapshot,
            SchemaElementLookup schema,
            SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> sequentials,
            Map<ZNodePath, CommittedChildIterator> committed,
            FutureCallback<?> callback,
            Function<? super Long, ? extends T> executors,
            Materializer<StorageZNode<?>,O> materializer) {
        ImmutableMap.Builder<ZNodePath, SequentialParentIterator> parents = ImmutableMap.builder();
        synchronized (committed) {
            for (Map.Entry<ZNodePath, CommittedChildIterator> entry: committed.entrySet()) {
                parents.put(entry.getKey(), SequentialParentIterator.create(entry.getValue()));
            }
        }
        Map<ZNodePath, SequentialParentIterator> iterators = parents.build();
        Function<ZNodePath,Records.MultiOpRequest> commit;
        try {
            commit = Functions.compose(
                    new Function<List<Records.Request>, Records.MultiOpRequest>() {
                        @Override
                        public Records.MultiOpRequest apply(List<Request> input) {
                            return (Records.MultiOpRequest) Iterables.getOnlyElement(input);
                        }
                    },
                    Functions.compose(
                            PathToRequests.forRequests(
                                    Operations.Requests.create()
                                    .setData(materializer.codec().toBytes(Boolean.TRUE))),
                            JoinToPath.forName(
                                    StorageZNode.CommitZNode.LABEL)));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return new RecreateEphemerals<O,T>(
                snapshot, 
                schema, 
                commit, 
                sequentials, 
                iterators, 
                materializer, 
                callback, 
                executors);
    }
    
    private static final AsyncFunction<Operation.ProtocolResponse<?>, List<Records.MultiOpRequest>> MAYBE_NODE_EXISTS = new AsyncFunction<Operation.ProtocolResponse<?>, List<Records.MultiOpRequest>>() {
        private final ListenableFuture<List<Records.MultiOpRequest>> empty = Futures.<List<Records.MultiOpRequest>>immediateFuture(ImmutableList.<Records.MultiOpRequest>of());
        @Override
        public ListenableFuture<List<Records.MultiOpRequest>> apply(
                Operation.ProtocolResponse<?> input) throws Exception {
            Operations.maybeError(input.record(), KeeperException.Code.NODEEXISTS);
            return empty;
        }
    };
    
    private final Materializer<StorageZNode<?>,O> materializer;
    private final Function<ZNodePath, Records.MultiOpRequest> commit;
    private final SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> sequentials;
    private final Map<ZNodePath, SequentialParentIterator> iterators;
    
    protected RecreateEphemerals(
            ZNodePath snapshot,
            SchemaElementLookup schema,
            Function<ZNodePath,Records.MultiOpRequest> commit,
            SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>> sequentials,
            Map<ZNodePath, SequentialParentIterator> iterators,
            Materializer<StorageZNode<?>,O> materializer,
            FutureCallback<?> callback,
            Function<? super Long, ? extends T> executors) {
        super(snapshot,
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral.class, 
                schema,
                callback, 
                executors);
        this.materializer = materializer;
        this.sequentials = sequentials;
        this.iterators = iterators;
        this.commit = commit;
    }
    
    @Override
    public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral ephemeral) {
        if ((ephemeral.data().stamp() < 0L) || (ephemeral.commit() != null)) {
            return;
        }
        Long id = Long.valueOf(((StorageZNode.SessionZNode<?>) ephemeral.parent().get().parent().get()).name().longValue());
        T executor = getExecutor(id);
        if (executor == null) {
            return;
        }
        final ICreateRequest record;
        try {
            record = (ICreateRequest) Records.Requests.deserialize(OpCode.CREATE, 
                    new ByteBufInputArchive(Unpooled.wrappedBuffer(ephemeral.data().get())));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        if (CreateMode.valueOf(record.getFlags()).contains(CreateFlag.SEQUENTIAL)) {
            AbsoluteZNodePath path = (AbsoluteZNodePath) ZNodePath.root().join(ZNodeName.fromString(ephemeral.name()));
            SequentialNode<AbsoluteZNodePath> sequential = sequentials.get(path);
            new RecreateSequentialEphemeral(
                    id,
                    record,
                    sequential,
                    iterators.get(sequential.parent().get().path()).submit(sequential));
        } else {
            new CommitCallback(
                    (AbsoluteZNodePath) ephemeral.path(), 
                    Futures.transform(
                            executor.submit(record), 
                            MAYBE_NODE_EXISTS)).run();
        }
    }
    
    protected final class RecreateSequentialEphemeral extends ChainedTask<Boolean> {
        
        private final SequentialNode<AbsoluteZNodePath> sequential;
        private final ICreateRequest record;
        private final Long id;
        
        protected RecreateSequentialEphemeral(
                Long id,
                ICreateRequest record,
                SequentialNode<AbsoluteZNodePath> sequential,
                ListenableFuture<?> future) {
            super(future);
            this.sequential = sequential;
            this.record = record;
            this.id = id;
            future.addListener(delegate, MoreExecutors.directExecutor());
        }
        
        public T executor() throws KeeperException {
            T executor = getExecutor(id);
            if (executor == null) {
                throw new KeeperException.SessionMovedException();
            }
            return executor;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                throw new AssertionError();
            }
            case 1:
            {
                Boolean committed = (Boolean) input.getLast().get();
                if (committed.booleanValue()) {
                    return Optional.absent();
                } else {
                    // We need to protect against the case that a sequential ephemeral has
                    // already been recreated, but the commit znode wasn't created.
                    // So, first we get existing sequential ephemeral children and 
                    // match against them to determine if this is the case
                    T executor = executor();
                    ZNodePath parent = sequential.parent().get().path();
                    PathToRequests getChildren = PathToRequests.forRequests(Operations.Requests.sync(), Operations.Requests.getChildren());
                    return Optional.of(
                            new GetExistingSequentialEphemeral(
                                    SubmittedRequests.submit(executor,
                                            getChildren.apply(parent))));
                }
            }
            case 2:
            {
                Optional<? extends Sequential<String,?>> existing = (Optional<? extends Sequential<String,?>>) input.getLast().get();
                ListenableFuture<? extends Sequential<String,?>> future;
                if (existing.isPresent()) {
                    future = Futures.immediateFuture(existing.get());
                } else {
                    future = GetCreatedLabel.create(
                            executor().submit(record));
                }
                ZNodePath path = sequential.getValue();
                CommitCallback commit = 
                        new CommitCallback(
                                path, 
                                Futures.transform(future, 
                                        new CreateSuffix(path)));
                commit.run();
                return Optional.of(commit);
            }
            default:
                break;
            }
            return Optional.absent();
        }
        
        protected final class GetExistingSequentialEphemeral extends ChainedTask<Optional<? extends Sequential<String,?>>> {

            protected GetExistingSequentialEphemeral(
                    ListenableFuture<?> future) {
                super(future);
                future.addListener(delegate, MoreExecutors.directExecutor());
            }

            @Override
            public Optional<? extends ListenableFuture<?>> apply(
                    FutureListChain<ListenableFuture<?>> input)
                    throws Exception {
                switch (input.size()) {
                case 0:
                {
                    throw new AssertionError();
                }
                case 1:
                {
                    @SuppressWarnings("unchecked")
                    List<String> children = ((Records.ChildrenGetter) Operations.unlessError(((List<O>) input.getLast().get()).get(1).record())).getChildren();
                    @SuppressWarnings("unchecked")
                    PathToRequests exists = PathToRequests.forRequests(Operations.Requests.sync(), Operations.Requests.exists());
                    List<Records.Request> requests = Lists.newArrayListWithCapacity(children.size()*2);
                    for (String child: children) {
                        Optional<? extends Sequential<String,?>> maybeSequential = Sequential.maybeFromString(child);
                        if (maybeSequential.isPresent()) {
                            requests.addAll(exists.apply(sequential.parent().get().path().join(ZNodeLabel.fromString(child))));
                        }
                    }
                    TaskExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>> executor = executor();
                    return Optional.of(SubmittedRequests.submit(executor, requests));
                }
                case 2:
                {
                    @SuppressWarnings("unchecked")
                    SubmittedRequests<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>> last = (SubmittedRequests<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>>) input.getLast();
                    List<Sequential<String,?>> sequentialEphemerals = Lists.newArrayListWithCapacity(last.getValue().size()/2);
                    for (int i=1; i<last.getValue().size(); i+=2) {
                        ZNodePath path = ZNodePath.fromString(((Records.PathGetter) last.getValue().get(i).getRequest().record()).getPath());
                        long owner = ((Records.StatGetter) Operations.unlessError(last.get().get(i).record())).getStat().getEphemeralOwner();
                        if (owner != Stats.CreateStat.ephemeralOwnerNone()) {
                            sequentialEphemerals.add(Sequential.fromString(path.label().toString()));
                        }
                    }
                    Collections.sort(sequentialEphemerals);
                    int index = 0;
                    Iterator<SequentialNode<AbsoluteZNodePath>> siblings = sequential.parent().get().values().iterator();
                    while ((siblings.next()) != sequential) {
                        ++index; 
                    }
                    Optional<? extends Sequential<String,?>> existing;
                    if (sequentialEphemerals.size() >= index+1) {
                        existing = Optional.of(sequentialEphemerals.get(index));
                    } else {
                        existing = Optional.absent();
                    }
                    return Optional.of(Futures.immediateFuture(existing));
                }
                default:
                    break;
                }
                return Optional.absent();
            }
        }
        
        protected final class CreateSuffix implements Function<Sequential<String,?>, List<Records.MultiOpRequest>> {

            private final Operations.Requests.Create create;
            
            protected CreateSuffix(
                    ZNodePath path) {
                this.create = Operations.Requests.create().setPath(path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral.Sequence.LABEL));
            }
            
            @Override
            public List<Records.MultiOpRequest> apply(Sequential<String, ?> input) {
                try {
                    return ImmutableList.of((Records.MultiOpRequest) create.setData(materializer.codec().toBytes(input.sequence())).build());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }
    
    protected final class CommitCallback extends SimpleToStringListenableFuture<Boolean> implements Runnable {
        
        protected CommitCallback(
                ZNodePath path,
                ListenableFuture<? extends List<? extends Records.MultiOpRequest>> future) {
            super(Commit.create(
                    commit.apply(path), 
                    materializer,
                    future));
        }

        @Override
        public void run() {
            if (isDone()) {
                try {
                    get();
                } catch (Exception e) {
                    onFailure(e);
                }
            } else {
                addListener(this, MoreExecutors.directExecutor());
            }
        }
    }
    
    protected static final class SequentialParentIterator implements Runnable {

        /**
         * Assumes it has exclusive access to the iterator.
         */
        public static SequentialParentIterator create(
                CommittedChildIterator iterator) {
            return new SequentialParentIterator(
                    iterator);
        }
        
        private final PriorityQueue<WaitingChild> waiting;
        private final CommittedChildIterator committed;
        
        protected SequentialParentIterator(
                CommittedChildIterator committed) {
            this.committed = committed;
            this.waiting = new PriorityQueue<WaitingChild>(committed.parent().size());
        }
        
        public synchronized ListenableFuture<Boolean> submit(SequentialNode<AbsoluteZNodePath> request) {
            WaitingChild task = new WaitingChild(request, SettableFuturePromise.<Boolean>create());
            waiting.add(task);
            run();
            return task;
        }

        @Override
        public synchronized void run() {
            WaitingChild next;
            while ((next = waiting.peek()) != null) {
                while (!next.isDone() && committed.hasNext()) {
                    CommittedChildIterator.NextChild<?> child = committed.peek();
                    if (child.getNode() == next.task()) {
                        next.set(Boolean.valueOf(child.getCommitted().isDone()));
                    }
                    if (child.getCommitted().isDone()) {
                        try {
                            child.getCommitted().get();
                        } catch (Exception e) {
                            next.setException(e);
                        }
                        committed.next();
                    } else if (!next.isDone()) {
                        child.getCommitted().addListener(this, MoreExecutors.directExecutor());
                        break;
                    }
                }
                if (!next.isDone() && !committed.hasNext()) {
                    next.set(Boolean.TRUE);
                }
                if (next.isDone()) {
                    waiting.remove(next);
                } else {
                    break;
                }
            }
        }
        
        protected static final class WaitingChild extends PromiseTask<SequentialNode<AbsoluteZNodePath>, Boolean> implements Comparable<WaitingChild> {

            protected WaitingChild(SequentialNode<AbsoluteZNodePath> task,
                    Promise<Boolean> delegate) {
                super(task, delegate);
            }

            @Override
            public int compareTo(WaitingChild o) {
                return SequentialNode.comparator().compare(
                        task().parent().name(), 
                        o.task().parent().name());
            }
        }
    }
    
    protected abstract static class ChainedTask<V> extends ToStringListenableFuture<V> implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>> {
    
        protected final ChainedFutures.ChainedFuturesTask<V> delegate;
        
        protected ChainedTask(
                ListenableFuture<?> future) {
            this.delegate = ChainedFutures.task(
                    ChainedFutures.<V>castLast(
                        ChainedFutures.apply(
                                this, 
                                ChainedFutures.list(
                                        Lists.<ListenableFuture<?>>newArrayList(ImmutableList.of(future))))));
        }
    
        @Override
        protected ListenableFuture<V> delegate() {
            return delegate;
        }
    }

    protected static final class Commit<O extends Operation.ProtocolResponse<?>> extends ChainedTask<Boolean> implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>> {
        
        public static <O extends Operation.ProtocolResponse<?>> Commit<O> create(
                Records.MultiOpRequest commit,
                TaskExecutor<? super Records.Request, O> client,
                ListenableFuture<? extends List<? extends Records.MultiOpRequest>> future) {
            return new Commit<O>(commit, client, future);
        }
        
        private final TaskExecutor<? super Records.Request, O> client;
        private final Records.MultiOpRequest commit;
        
        protected Commit(
                Records.MultiOpRequest commit,
                TaskExecutor<? super Records.Request, O> client,
                ListenableFuture<? extends List<? extends Records.MultiOpRequest>> future) {
            super(future);
            this.commit = commit;
            this.client = client;
            future.addListener(delegate, MoreExecutors.directExecutor());
        }

        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                throw new AssertionError();
            }
            case 1:
            {
                @SuppressWarnings("unchecked")
                List<? extends Records.MultiOpRequest> requests = (List<? extends Records.MultiOpRequest>) input.getLast().get();
                Records.Request request;
                if (requests.isEmpty()) {
                    request = commit;
                } else {
                    request = new IMultiRequest(
                            ImmutableList.<Records.MultiOpRequest>builder().addAll(requests).add(commit).build());
                }
                return Optional.of(client.submit(request));
            }
            case 2:
            {
                @SuppressWarnings("unchecked")
                Records.Response response = ((O) input.getLast().get()).record();
                Optional<Operation.Error> error;
                if (response instanceof IMultiResponse) {
                    error = Operations.maybeMultiError((IMultiResponse) response, KeeperException.Code.NODEEXISTS);
                } else {
                    error = Operations.maybeError(response, KeeperException.Code.NODEEXISTS);
                }
                return Optional.of(Futures.immediateFuture(Boolean.valueOf(!error.isPresent())));
            }
            default:
                break;
            }
            return Optional.absent();
        }
    }

    protected static final class GetCreatedLabel<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<O> implements Callable<Optional<Sequential<String,?>>> {
    
        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<? extends Sequential<String,?>> create(
                ListenableFuture<O> future) {
            return CallablePromiseTask.listen(
                    new GetCreatedLabel<O>(future), 
                    SettableFuturePromise.<Sequential<String,?>>create());
        }
        
        protected GetCreatedLabel(ListenableFuture<O> delegate) {
            super(delegate);
        }
    
        @Override
        public Optional<Sequential<String,?>> call() throws Exception {
            if (isDone()) {
                return Optional.<Sequential<String,?>>of(
                        Sequential.fromString(
                                ZNodePath.fromString(
                                        ((Records.PathGetter) Operations.unlessError(get().record())).getPath())
                                        .label().toString()));
            }
            return Optional.absent();
        }
    }
}
