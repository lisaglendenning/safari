package edu.uw.zookeeper.safari.storage.snapshot;

import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.WatchType;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.client.Watchers.CacheNodeCreatedListener;
import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ListenableFutureActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.SubmitActor;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.OptionalNode;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ICreateRequest;
import edu.uw.zookeeper.protocol.proto.IDeleteRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.SessionsWatcher;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;


public final class SnapshotSessionExecutors<O extends Operation.ProtocolResponse<?>> extends CacheNodeCreatedListener<StorageZNode<?>> {

    @SuppressWarnings("unchecked")
    public static <O extends Operation.ProtocolResponse<?>> SnapshotSessionExecutors<O> listen(
            final Service service,
            final SchemaClientService<StorageZNode<?>,O> client,
            final Function<? super Long, ? extends TaskExecutor<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>>> executors) {
        final Predicate<Long> isLocal = new Predicate<Long>() {
            @Override
            public boolean apply(Long session) {
                return (executors.apply(session) != null);
            }
        };
        SnapshotSessionExecutors<O> instance = new SnapshotSessionExecutors<O>(client, isLocal, executors, service);
        instance.listen();
        final Predicate<StorageZNode.SessionZNode<?>> sessionIsLocal = new Predicate<StorageZNode.SessionZNode<?>>() {
            @Override
            public boolean apply(StorageZNode.SessionZNode<?> input) {
                return isLocal.apply(Long.valueOf(input.name().longValue()));
            }
        };
        final Predicate<StorageZNode.EscapedNamedZNode<?>> getZnode = new Predicate<StorageZNode.EscapedNamedZNode<?>>() {
            @Override
            public boolean apply(StorageZNode.EscapedNamedZNode<?> input) {
                return (input.data().stamp() < 0L) && sessionIsLocal.apply((StorageZNode.SessionZNode<?>) input.parent().get());
            }
        };
        ImmutableList.Builder<WatchMatchListener> listeners = ImmutableList.builder();
        for (Class<? extends StorageZNode.EscapedNamedZNode<?>> type: ImmutableList.of(
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral.class,
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Session.Watch.class)) {
            listeners.add(FilteredCallback.forRequests(
                            type,
                            getZnode, 
                            client.materializer(),
                            instance.logger(),
                            instance.queryProcessor,
                            instance.queryCallback,
                            Operations.Requests.sync(),
                            Operations.Requests.getData()));
        }
        listeners.add(Watchers.FutureCallbackListener.create(
                Watchers.EventToPathCallback.create(
                        instance.new SessionReconnectListener()), 
                WatchMatcher.exact(
                        StorageSchema.Safari.Sessions.Session.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDataChanged), 
                        instance.logger()));
        for (WatchMatchListener listener: listeners.build()) {
            Watchers.CacheNodeCreatedListener.listen(
                    client.materializer().cache(), 
                    service, 
                    client.cacheEvents(), 
                    listener, 
                    instance.logger());
        }
        SessionsWatcher.listen(client, service);
        return instance;
    }

    protected final SchemaClientService<StorageZNode<?>,O> client;
    protected final Predicate<Long> isLocal;
    protected final Function<? super Long, ? extends TaskExecutor<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>>> executors;
    protected final ConcurrentMap<ZNodePath, SnapshotListener> snapshots;
    protected final Watchers.MaybeErrorProcessor queryProcessor;
    protected final Watchers.FailWatchListener<Object> queryCallback;
    
    protected SnapshotSessionExecutors(
            SchemaClientService<StorageZNode<?>,O> client,
            Predicate<Long> isLocal,
            Function<? super Long, ? extends TaskExecutor<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>>> executors,
            Service service) {
        super(client.materializer().cache(), 
                service, 
                client.cacheEvents(),
                WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH,
                        Watcher.Event.EventType.NodeDataChanged));
        this.client = client;
        this.isLocal = isLocal;
        this.executors = executors;
        this.snapshots = new MapMaker().makeMap();
        this.queryProcessor = Watchers.MaybeErrorProcessor.maybeNoNode();
        this.queryCallback = Watchers.FailWatchListener.create(this);
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        super.handleWatchEvent(event);
        final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit commit = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit)
                cache.cache().get(event.getPath());
        if (commit.data().get().booleanValue() && !snapshots.containsKey(commit.snapshot().path())) {
            SnapshotListener snapshot = new SnapshotListener((AbsoluteZNodePath) commit.snapshot().path());
            SnapshotListener existing = snapshots.putIfAbsent(snapshot.getWatchMatcher().getPath(), snapshot);
            assert (existing == null);
            if (isRunning()) {
                snapshot.listen();
            } else {
                switch (state()) {
                case STOPPING:
                case TERMINATED:
                    snapshot.stopping(Service.State.RUNNING);
                    snapshot.terminated(Service.State.STOPPING);
                    break;
                case FAILED:
                    snapshot.failed(Service.State.RUNNING, service.failureCause());
                    break;
                default:
                    break;
                }
            }
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        Services.stop(service);
    }
    
    protected final class SnapshotListener extends WatchMatchServiceListener {

        protected final AbsoluteZNodePath root;
        protected final DeleteProcessor deletes;
        protected final RecreateProcessor recreates;
        protected final SequentialProcessor sequentials;
        protected final WatchProcessor watches;
        
        public SnapshotListener(AbsoluteZNodePath path) {
            super(SnapshotSessionExecutors.this.service, 
                    client.notifications(), 
                    WatchMatcher.prefix(
                            path, 
                            Watcher.Event.EventType.NodeDeleted, 
                            Watcher.Event.EventType.NodeDataChanged));
            this.root = path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL);
            this.deletes = new DeleteProcessor();
            this.recreates = new RecreateProcessor();
            this.sequentials = new SequentialProcessor();
            this.watches = new WatchProcessor();
        }

        @Override
        public void listen() {
            starting();
            running();
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            switch (event.getEventType()) {
            case NodeDeleted:
            {
                final ValueNode<ZNodeSchema> schema = ZNodeSchema.matchPath(client.materializer().schema().get(), event.getPath());
                if (schema.get().getDeclaration() == StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.class) {
                    stopping(Service.State.RUNNING);
                    terminated(Service.State.STOPPING);
                } else if (schema.get().getDeclaration() == StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.class) {
                    watches.run();
                } else if (schema.get().getDeclaration() == StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral.class) {
                    final AbsoluteZNodePath unescaped = (AbsoluteZNodePath) ZNodePath.root().join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral.converter().reverse().convert(event.getPath().label()));
                    sequentials.trie().remove(unescaped);
                }
                break;
            }
            case NodeDataChanged:
            {
                StorageZNode<?> node = cache.cache().get(event.getPath());
                if (node instanceof StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral) {
                    final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral ephemeral = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral) node;
                    ICreateRequest record;
                    try {
                        record = (ICreateRequest) Records.Requests.deserialize(OpCode.CREATE, 
                                new ByteBufInputArchive(Unpooled.wrappedBuffer(ephemeral.data().get())));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                    final Recreate recreate = new Recreate(
                            ephemeral.session().name().longValue(), 
                            (AbsoluteZNodePath) node.path(), 
                            record);
                    try {
                        if (CreateMode.valueOf(record.getFlags()).contains(CreateFlag.SEQUENTIAL)) {
                            sequentials.send(recreate);
                        } else {
                            recreates.submit(recreate);
                        }
                    } catch (RejectedExecutionException e) {}
                } else if (node instanceof StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Session.Watch) {
                    final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Session.Watch watch = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Session.Watch) node;
                    try {
                        watches.send(Pair.create((AbsoluteZNodePath) node.path(),
                                new Watch(watch.session().name().longValue(), 
                                        root.join(watch.name()), 
                                        watch.data().get())));
                    } catch (RejectedExecutionException e) {}
                }
                break;
            }
            default:
                break;
            }
        }
        
        @Override
        public void running() {
            super.running();
            cache.lock().readLock().lock();
            try {
                final ZNodePath path = getWatchMatcher().getPath();
                final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = 
                        (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) cache.cache().get(path);
                if (snapshot != null) {
                    @SuppressWarnings("unchecked")
                    final PathToRequests query = PathToRequests.forRequests(
                            Operations.Requests.sync(), 
                            Operations.Requests.getChildren());
                    if (snapshot.ephemerals() != null) {
                        synchronized (sequentials.trie()) {
                            for (StorageZNode<?> session: snapshot.ephemerals().values()) {
                                sequentials.trie().add(SubmittedRequests.submit(
                                        client.materializer(), 
                                        query.apply(session.path())));
                            }
                        }
                    }
                    sequentials.trie().run();
                    if (snapshot.watches() != null) {
                        final ImmutableList.Builder<Records.Request> builder = ImmutableList.builder();
                        for (StorageZNode<?> child: snapshot.watches().values()) {
                            StorageZNode.SessionZNode<?> session = (StorageZNode.SessionZNode<?>) child;
                            if (isLocal.apply(Long.valueOf(session.name().longValue()))) {
                                builder.addAll(query.apply(session.path()));
                            }
                        }
                        ImmutableList<Records.Request> requests = builder.build();
                        if (!requests.isEmpty()) {
                            Watchers.Query.create(
                                    queryProcessor,
                                    queryCallback,
                                    SubmittedRequests.submit(client.materializer(), requests));
                        }
                    }
                } else {
                    handleWatchEvent(NodeWatchEvent.nodeDeleted(path));
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        
        @Override
        public void terminated(Service.State from) {
            super.terminated(from);
            stop();
        }
        
        @Override
        public void failed(Service.State from, Throwable failure) {
            super.terminated(from);
            stop();
        }
        
        protected void stop() {
            snapshots.remove(getWatchMatcher().getPath(), this);
            sequentials.stop();
            watches.stop();
            recreates.stop();
            deletes.stop();
        }
            
        protected final class SequentialTrieBuilder extends ForwardingListenableFuture<SimpleLabelTrie<OptionalNode<Recreate>>> implements Callable<Optional<SimpleLabelTrie<OptionalNode<Recreate>>>>, Runnable {

            protected final Queue<SubmittedRequests<? extends Records.Request, O>> requests;
            protected final SimpleLabelTrie<OptionalNode<Recreate>> trie;
            protected final PathToQuery<?,O> query;
            protected final CallablePromiseTask<SequentialTrieBuilder, SimpleLabelTrie<OptionalNode<Recreate>>> delegate;
           
            @SuppressWarnings("unchecked")
            public SequentialTrieBuilder() {
                this.trie = SimpleLabelTrie.<OptionalNode<Recreate>>forRoot(SequentialNode.sequentialRoot());;
                this.query = PathToQuery.forRequests(
                        client.materializer(), 
                        Operations.Requests.sync(), 
                        Operations.Requests.exists().setWatch(true));
                this.requests = Queues.newArrayDeque();
                this.delegate = CallablePromiseTask.create(this, SettableFuturePromise.<SimpleLabelTrie<OptionalNode<Recreate>>>create());
                delegate.addListener(this, MoreExecutors.directExecutor());
            }
            
            @Override
            public synchronized void run() {
                delegate.run();
                if (isDone()) {
                    ListenableFuture<?> next;
                    while ((next = requests.poll()) != null) {
                        next.cancel(false);
                    }
                } else {
                    this.requests.peek().addListener(this, MoreExecutors.directExecutor());
                }
            }

            @Override
            public synchronized Optional<SimpleLabelTrie<OptionalNode<Recreate>>> call()
                    throws Exception {
                SubmittedRequests<? extends Records.Request, O> next = this.requests.peek();
                if (next == null) {
                    return Optional.of(trie);
                } else if (next.isDone() && requests.remove(next)) {
                    final Records.Response response = next.get().get(next.requests().size()-1).record();
                    final Optional<Operation.Error> error = Operations.maybeError(response, KeeperException.Code.NONODE);
                    if (!error.isPresent()) {
                        final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) next.requests().get(next.requests().size()-1)).getPath());
                        cache.lock().readLock().lock();
                        try {
                            final StorageZNode<?> session = cache.cache().get(path);
                            for (String child: ((Records.ChildrenGetter) response).getChildren()) {
                                final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral ephemeral = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.Ephemeral) session.get(ZNodeLabel.fromString(child));
                                final int index = ephemeral.name().toString().lastIndexOf(ZNodeName.SLASH);
                                final String label = (index >= 0) ? ephemeral.name().toString().substring(index+1) : ephemeral.name().toString();
                                final Optional<? extends Sequential<String, ?>> sequential = Sequential.maybeFromString(label);
                                if (sequential.isPresent()) {
                                    SequentialNode.putIfAbsent(trie, ZNodePath.root().join(ephemeral.name()));
                                    Watchers.Query.call(
                                            queryProcessor,
                                            queryCallback, 
                                            query.apply(path.join(ephemeral.parent().name())));
                                }
                            }
                        } finally {
                            cache.lock().readLock().unlock();
                        }
                    }
                }
                return Optional.absent();
            }
            
            public synchronized void add(SubmittedRequests<? extends Records.Request, O> request) {
                requests.add(request);
            }
            
            public synchronized void remove(AbsoluteZNodePath path) {
                OptionalNode<Recreate> node = trie.get(path);
                assert (node.isEmpty());
                node.remove();
                node = node.parent().get();
                if (node.isEmpty()) {
                    while ((node != trie.root()) && node.isEmpty() && node.remove()) {
                        node = node.parent().get();
                    }
                } else {
                    node = node.values().iterator().next();
                    if (node.isPresent()) {
                        sequentials.send(node.get());
                    }
                }
            }
            
            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this).addValue(ToStringListenableFuture.toString3rdParty(this)).toString();
            }

            @Override
            protected ListenableFuture<SimpleLabelTrie<OptionalNode<Recreate>>> delegate() {
                return delegate;
            }
        }
        
        protected final class SequentialProcessor extends Actors.QueuedActor<Recreate> {

            protected final SequentialTrieBuilder trie;
            
            public SequentialProcessor() {
                super(Queues.<Recreate>newConcurrentLinkedQueue(), 
                        LogManager.getLogger(SequentialProcessor.class));
                this.trie = new SequentialTrieBuilder();
                trie.addListener(this, MoreExecutors.directExecutor());
            }
            
            public SequentialTrieBuilder trie() {
                return trie;
            }
            
            @Override
            public boolean isReady() {
                return (!mailbox.isEmpty() && trie.isDone());
            }
            
            @Override
            protected boolean apply(Recreate recreate) throws Exception {
                assert (trie.isDone());
                if (mailbox.remove(recreate)) {
                    SimpleLabelTrie<OptionalNode<Recreate>> trie = this.trie.get();
                    OptionalNode<Recreate> node = trie.get(recreate.path().suffix(root));
                    if (!node.isPresent()) {
                        node.set(recreate);
                    } else {
                        assert (node.get() == recreate);
                    }
                    while (node.isPresent() && (node.parent().get().values().iterator().next() == node)) {
                        node.remove();
                        node = node.parent().get();
                        try {
                            recreates.submit(node.get());
                        } catch (RejectedExecutionException e) {}
                        if (node.isEmpty()) {
                            do {
                                node.remove();
                                node = node.parent().get();
                            } while ((node != trie.root()) && node.isEmpty());
                            break;
                        } else {
                            node = node.values().iterator().next();
                        }
                    }
                    return true;
                }
                return false;
            }
        }
        
        protected final class WatchProcessor extends Actors.QueuedActor<Pair<AbsoluteZNodePath, Watch>> {

            private final ZNodePath ephemeralsPath;
            private final ImmutableMap<WatchType, Operations.PathBuilder<? extends Records.Request, ?>> requests;
            
            public WatchProcessor() {
                super(Queues.<Pair<AbsoluteZNodePath, Watch>>newConcurrentLinkedQueue(), 
                        LogManager.getLogger(WatchProcessor.class));
                this.ephemeralsPath = getWatchMatcher().getPath().join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL);
                this.requests = ImmutableMap.
                        <WatchType, Operations.PathBuilder<? extends Records.Request, ?>>of(WatchType.DATA, Operations.Requests.exists().setWatch(true));
            }
            
            @Override
            public boolean isReady() {
                if (mailbox.isEmpty()) {
                    return false;
                }
                client.materializer().cache().lock().readLock().lock();
                try {
                    return !client.materializer().cache().cache().containsKey(ephemeralsPath);
                } finally {
                    client.materializer().cache().lock().readLock().unlock();
                }
            }
            
            @Override
            protected boolean apply(Pair<AbsoluteZNodePath, Watch> input) throws Exception {
                final Watch watch = input.second();
                final Long session = Long.valueOf(input.second().session);
                Records.Request request = requests.get(input.second().type()).setPath(watch.path()).build();
                try {
                    recreates.submit(new Recreate(session, input.first(), request));
                } catch (RejectedExecutionException e) {}
                return true;
            }
        }

        protected final class RecreateProcessor extends ListenableFutureActor<Recreate, ShardedServerResponseMessage<?>, FutureValue<AbsoluteZNodePath,ShardedServerResponseMessage<?>>> {

            public RecreateProcessor() {
                super(Queues.<FutureValue<AbsoluteZNodePath,ShardedServerResponseMessage<?>>>newConcurrentLinkedQueue(), LogManager.getLogger(RecreateProcessor.class));
            }

            @Override
            public ListenableFuture<ShardedServerResponseMessage<?>> submit(
                    Recreate request) {
                TaskExecutor<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>> executor = executors.apply(Long.valueOf(request.session()));
                if (executor != null) {
                    final ListenableFuture<ShardedServerResponseMessage<?>> future = executor.submit(
                            ShardedRequestMessage.valueOf(
                                    VersionedId.zero(), 
                                    ProtocolRequestMessage.of(0, request.request())));
                    if (!send(FutureValue.forValue(request.path(), future))) {
                        future.cancel(false);
                    }
                    return future;
                } else {
                    throw new RejectedExecutionException();
                }
            }

            @Override
            protected boolean doApply(
                    FutureValue<AbsoluteZNodePath,ShardedServerResponseMessage<?>> input)
                    throws Exception {
                final Records.Response response = input.get().getResponse().record();
                switch (response.opcode()) {
                case EXISTS:
                case GET_CHILDREN:
                    Operations.maybeError(response, KeeperException.Code.NONODE);
                    break;
                default:
                    Operations.unlessError(response);
                }
                deletes.submit(input.value());
                return true;
            }
        }
        
        protected final class DeleteProcessor extends SubmitActor<IDeleteRequest,O> {

            private final Operations.Requests.Delete delete;
            
            public DeleteProcessor() {
                super(SnapshotSessionExecutors.this.client.materializer(), 
                        Queues.<SubmittedRequest<IDeleteRequest,O>>newConcurrentLinkedQueue(), 
                        LogManager.getLogger(DeleteProcessor.class));
                this.delete = Operations.Requests.delete();
            }
            
            public synchronized ListenableFuture<O> submit(AbsoluteZNodePath path) {
                return submit(delete.setPath(path).build());
            }

            @Override
            protected boolean doApply(SubmittedRequest<IDeleteRequest,O> input) throws Exception {
                super.doApply(input);
                SnapshotSessionExecutors.this.client.materializer().cache().lock().readLock().lock();
                try {
                    ZNodePath path = AbsoluteZNodePath.fromString(input.request().getPath()).parent();
                    StorageZNode<?> node = SnapshotSessionExecutors.this.client.materializer().cache().cache().get(path);
                    if ((node instanceof StorageZNode.SessionZNode) && node.isEmpty()) {
                        submit((AbsoluteZNodePath) node.path());
                    }
                } finally {
                    SnapshotSessionExecutors.this.client.materializer().cache().lock().readLock().unlock();
                }
                return true;
            }
        }
    }
    
    protected final class SessionReconnectListener implements FutureCallback<ZNodePath> {

        protected final Watchers.MaybeErrorProcessor processor;
        protected final FutureCallback<Object> callback;
        
        protected SessionReconnectListener() {
            super();
            this.processor = Watchers.MaybeErrorProcessor.maybeNoNode();
            this.callback = Watchers.FailWatchListener.create(SnapshotSessionExecutors.this);
        }

        @Override
        public void onSuccess(ZNodePath result) {
            final StorageSchema.Safari.Sessions.Session session = (StorageSchema.Safari.Sessions.Session) cache.cache().get(result);
            if ((session.stat().stamp() > 0L) && (session.stat().get().getVersion() > 1) && isLocal.apply(Long.valueOf(session.name().longValue()))) {
                @SuppressWarnings("unchecked")
                final PathToRequests getChildren = PathToRequests.forRequests( 
                        Operations.Requests.sync(), 
                        Operations.Requests.getChildren());
                @SuppressWarnings("unchecked")
                final PathToRequests getData = PathToRequests.forRequests( 
                        Operations.Requests.sync(), 
                        Operations.Requests.getData());
                final ImmutableList.Builder<Records.Request> builder = ImmutableList.builder();
                for (ZNodePath path: snapshots.keySet()) {
                    StorageZNode<?> snapshot = client.materializer().cache().cache().get(path);
                    for (StorageZNode<?> state: snapshot.values()) {
                        StorageZNode<?> node = state.get(session.parent().name());
                        if (node != null) {
                            if (node.isEmpty()) {
                                builder.addAll(getChildren.apply(node.path()));
                            } else {
                                for (StorageZNode<?> znode: node.values()) {
                                    if (znode.data().stamp() < 0L) {
                                        builder.addAll(getData.apply(znode.path()));
                                    }
                                }
                            }
                        }
                    }
                }
                ImmutableList<Records.Request> requests = builder.build();
                if (!requests.isEmpty()) {
                    Watchers.Query.create(
                            processor,
                            callback,
                            SubmittedRequests.submit(client.materializer(), requests)).run();
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }

    protected static final class FilteredCallback<T extends StorageZNode<?>> implements FutureCallback<ZNodePath> {
        
        public static <T extends StorageZNode<?>, O extends Operation.ProtocolResponse<?>, V> Watchers.FutureCallbackListener<?> forRequests(
                Class<? extends T> type,
                Predicate<? super T> filter,
                Materializer<StorageZNode<?>,O> materializer,
                Logger logger,
                Processor<? super List<O>, V> processor,
                FutureCallback<? super V> callback,
                Operations.Builder<? extends Records.Request>... builders) {
            Watchers.PathToQueryCallback<?,?> delegate = Watchers.PathToQueryCallback.create(
                    PathToQuery.forRequests(
                            materializer, 
                            builders), 
                    processor, 
                    callback);
            return Watchers.FutureCallbackListener.create(
                    Watchers.EventToPathCallback.create(
                            create(filter, 
                                    delegate, 
                                    materializer.cache().cache())),
                    WatchMatcher.exact(
                            materializer.schema().apply(type).path(), 
                            Watcher.Event.EventType.NodeCreated), 
                    logger);
        }

        protected static <T extends StorageZNode<?>> FilteredCallback<T> create(
                Predicate<? super T> filter,
                FutureCallback<ZNodePath> delegate,
                NameTrie<StorageZNode<?>> trie) {
            return new FilteredCallback<T>(
                    filter, 
                    delegate, 
                    trie);
        }
        
        private final NameTrie<StorageZNode<?>> trie;
        private final Predicate<? super T> filter;
        private final FutureCallback<ZNodePath> delegate;
        
        protected FilteredCallback(
                Predicate<? super T> filter,
                FutureCallback<ZNodePath> delegate,
                NameTrie<StorageZNode<?>> trie) {
            this.filter = filter;
            this.delegate = delegate;
            this.trie = trie;
        }
    
        @Override
        public void onSuccess(ZNodePath path) {
            @SuppressWarnings("unchecked")
            final T node = (T) trie.get(path);
            if (filter.apply(node)) {
                delegate.onSuccess(path);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            delegate.onFailure(t);
        }
    }
    
    protected static final class FutureValue<U,V> extends SimpleToStringListenableFuture<V> {

        public static <U,V> FutureValue<U,V> forValue(U value,
                ListenableFuture<V> future) {
            return new FutureValue<U,V>(value, future);
        }
        
        protected final U value;
        
        protected FutureValue(
                U value,
                ListenableFuture<V> future) {
            super(future);
            this.value = value;
        }
        
        public U value() {
            return value;
        }
        
        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
            return super.toStringHelper(helper.addValue(value));
        }
    }

    protected static final class Recreate {

        private final long session;
        private final AbsoluteZNodePath path;
        private final Records.Request request;
        
        public Recreate(
                long session,
                AbsoluteZNodePath path, 
                Records.Request request) {
            this.session = session;
            this.path = path;
            this.request = request;
        }
        
        public long session() {
            return session;
        }
        
        public AbsoluteZNodePath path() {
            return path;
        }
        
        public Records.Request request() {
            return request;
        }
    }
    
    protected static final class Watch {

        private final long session;
        private final AbsoluteZNodePath path;
        private final WatchType type;
        
        public Watch(
                long session,
                AbsoluteZNodePath path, 
                WatchType type) {
            this.session = session;
            this.path = path;
            this.type = type;
        }
        
        public long session() {
            return session;
        }
        
        public AbsoluteZNodePath path() {
            return path;
        }
        
        public WatchType type() {
            return type;
        }
    }
    
    protected static final class SequentialNode extends OptionalNode<Recreate> {

        protected static final Comparator<ZNodeName> COMPARATOR = new  Comparator<ZNodeName>() {
            @Override
            public int compare(final ZNodeName a, final ZNodeName b) {
                final Optional<? extends Sequential<String, ?>> aSeq = Sequential.maybeFromString(a);
                final Optional<? extends Sequential<String, ?>> bSeq = Sequential.maybeFromString(b);
                if (aSeq.isPresent()) {
                    if (bSeq.isPresent()) {
                        return aSeq.get().compareTo(bSeq.get());
                    } else {
                        return -1;
                    }
                } else {
                    if (bSeq.isPresent()) {
                        return 1;
                    } else {
                        return a.toString().compareTo(b.toString());
                    }
                }
            }
        };
        
        public static SequentialNode sequentialRoot() {
            return new SequentialNode(
                    Optional.<Recreate>absent(),
                    AbstractNameTrie.<OptionalNode<Recreate>>rootPointer());
        }
        
        public static SequentialNode sequentialChild(ZNodeName label, SequentialNode parent) {
            return new SequentialNode(
                    Optional.<Recreate>absent(),
                    AbstractNameTrie.<OptionalNode<Recreate>>weakPointer(label, parent));
        }
        
        protected SequentialNode(
                Optional<Recreate> value,
                Pointer<? extends OptionalNode<Recreate>> parent) {
            super(value, parent, 
                    Maps.<ZNodeName, ZNodeName, OptionalNode<Recreate>>newTreeMap(COMPARATOR));
        }

        @Override
        protected SequentialNode newChild(ZNodeName label) {
            return sequentialChild(label, this);
        }
    }
}
