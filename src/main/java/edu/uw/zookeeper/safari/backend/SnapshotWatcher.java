package edu.uw.zookeeper.safari.backend;

import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.WatchType;
import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.TreeWalker;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.client.Watchers.CacheNodeCreatedListener;
import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.ListenableFutureActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SubmitActor;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.AbstractNameTrie;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.DefaultsNode;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie.Pointer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.ByteBufInputArchive;
import edu.uw.zookeeper.protocol.proto.ICreateRequest;
import edu.uw.zookeeper.protocol.proto.IDeleteRequest;
import edu.uw.zookeeper.protocol.proto.IGetChildrenRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;
import edu.uw.zookeeper.safari.storage.SessionsWatcher;
import edu.uw.zookeeper.safari.storage.StorageClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;


public class SnapshotWatcher extends CacheNodeCreatedListener<StorageZNode<?>> {

    public static SnapshotWatcher listen(
            final Service service,
            final StorageClientService storage,
            final BackendSessionExecutors executors) {
        SnapshotWatcher instance = new SnapshotWatcher(storage, executors, service);
        instance.listen();
        return instance;
    }

    protected final StorageClientService storage;
    protected final BackendSessionExecutors executors;
    
    protected SnapshotWatcher(
            StorageClientService storage,
            BackendSessionExecutors executors,
            Service service) {
        super(StorageSchema.Safari.Volumes.Volume.PATH, 
                storage.materializer().cache(), 
                service, 
                storage.cacheEvents());
        this.storage = storage;
        this.executors = executors;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        new VolumeSnapshotCommitted((AbsoluteZNodePath) event.getPath()).listen();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void starting() {
        final Predicate<Long> isLocal = new Predicate<Long>() {
            @Override
            public boolean apply(Long session) {
                return (executors.get(session) != null);
            }
        };
        final Predicate<StorageZNode.SessionZNode<?>> sessionIsLocal = new Predicate<StorageZNode.SessionZNode<?>>() {
            @Override
            public boolean apply(StorageZNode.SessionZNode<?> input) {
                return isLocal.apply(Long.valueOf(input.name().longValue()));
            }
        };
        final Predicate<StorageZNode.EscapedNamedZNode<?>> znodeIsLocal = new Predicate<StorageZNode.EscapedNamedZNode<?>>() {
            @Override
            public boolean apply(StorageZNode.EscapedNamedZNode<?> input) {
                return (input.data().stamp() < 0L) && sessionIsLocal.apply((StorageZNode.SessionZNode<?>) input.parent().get());
            }
        };
        for (Class<? extends StorageZNode.EscapedNamedZNode<?>> type: ImmutableList.of(
                StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.Ephemeral.class,
                StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.Session.Watch.class)) {
            QueryIfPredicate.listen(
                    znodeIsLocal, 
                    type, 
                    storage.materializer(), 
                    service, 
                    storage.cacheEvents(), 
                    Operations.Requests.sync(),
                    Operations.Requests.getData());
        }
        QueryIfPredicate.listen(
                sessionIsLocal, 
                StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.Session.class, 
                storage.materializer(), 
                service, 
                storage.cacheEvents(), 
                Operations.Requests.sync(),
                Operations.Requests.getChildren());
        super.starting();
        Watchers.childrenWatcher(
                StorageSchema.Safari.Volumes.PATH, 
                storage.materializer(), 
                executors, 
                storage.notifications());
        SessionsWatcher.listen(storage, service);
    }
    
    protected class VolumeSnapshotCommitted extends AbstractWatchListener implements Runnable {

        protected final AbsoluteZNodePath root;
        protected final FixedQuery<Message.ServerResponse<?>> query;
        protected final DeleteProcessor deletes;
        protected final RecreateProcessor recreates;
        protected final SequentialProcessor sequentials;
        protected final WatchProcessor watches;
        protected Optional<? extends ListenableFuture<Optional<SimpleLabelTrie<SequentialNode>>>> sequential;
        
        @SuppressWarnings("unchecked")
        public VolumeSnapshotCommitted(AbsoluteZNodePath path) {
            super(SnapshotWatcher.this.service, 
                    storage.notifications(), 
                    WatchMatcher.exact(
                            path.join(StorageSchema.Safari.Volumes.Volume.Snapshot.LABEL).join(StorageSchema.Safari.Volumes.Volume.Snapshot.Commit.LABEL), 
                            Watcher.Event.EventType.NodeCreated));
            this.root = path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL);
            this.query = FixedQuery.forIterable(
                    storage.materializer(), 
                    PathToRequests.forRequests(
                            Operations.Requests.sync(), 
                            Operations.Requests.getData().setWatch(true))
                        .apply(getWatchMatcher().getPath()));
            this.deletes = new DeleteProcessor();
            this.recreates = new RecreateProcessor();
            this.sequentials = new SequentialProcessor();
            this.watches = new WatchProcessor();
            this.sequential = Optional.absent();
        }
        
        @Override
        public void listen() {
            super.listen();
            Watchers.StopOnDeleteListener.listen(
                    this, 
                    root.parent());
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            run();
        }
        
        @Override
        public void run() {
            new Query();
        }
        
        @Override
        public void running() {
            run();
        }
        
        @Override
        public void stopping(Service.State from) {
            super.stopping(from);
            
            sequentials.stop();
            watches.stop();
            recreates.stop();
            deletes.stop();
        }
        
        protected class Query extends Watchers.Query {
            
            public Query() {
                super(VolumeSnapshotCommitted.this.query, VolumeSnapshotCommitted.this);
            }

            @Override
            public Optional<Operation.Error> call() throws Exception {
                Optional<Operation.Error> error = super.call();
                if (!error.isPresent()) {
                    cache.lock().readLock().lock();
                    try {
                        if (((Boolean) cache.cache().get(getWatchMatcher().getPath()).data().get()).booleanValue()) {
                            for (Class<? extends StorageZNode<?>> type : 
                                ImmutableList.<Class<? extends StorageZNode<?>>>of(
                                        StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.class,
                                        StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.class)) {
                                new SessionsWatcher<StorageZNode<?>>(type).listen();
                            }
                        }
                    } finally {
                        cache.lock().readLock().unlock();
                    }
                }
                return error;
            }
        }
        
        protected class SessionsWatcher<E extends StorageZNode<?>> extends AbstractWatchListener {

            protected final Class<? extends E> type;
                    
            public SessionsWatcher(
                    Class<? extends E> type) {
                super(VolumeSnapshotCommitted.this.service, 
                        VolumeSnapshotCommitted.this.watch, 
                        WatchMatcher.exact(
                                root.parent().join(storage.materializer().schema().apply(type).parent().name()), 
                                Watcher.Event.EventType.NodeDeleted));
                this.type = type;
            }

            @SuppressWarnings("unchecked")
            @Override
            public void handleWatchEvent(WatchEvent event) {
               Watchers.Query.call(
                        FixedQuery.forIterable(
                                storage.materializer(), 
                                PathToRequests.forRequests(
                                        Operations.Requests.sync(), 
                                        Operations.Requests.exists())
                                    .apply(event.getPath())), 
                        this);
                stopping(state());
            }
            
            @Override
            public void running() {
                new Query();
            }
            
            protected class Query extends Watchers.Query {

                @SuppressWarnings("unchecked")
                public Query() {
                    super(FixedQuery.forIterable(
                            storage.materializer(), 
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.exists().setWatch(true), 
                                    Operations.Requests.getChildren())
                                .apply(getWatchMatcher().getPath())), 
                        SessionsWatcher.this);
                }

                @Override
                public Optional<Operation.Error> call() throws Exception {
                    Optional<Operation.Error> error = super.call();
                    if (type == StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.class) {
                        synchronized (VolumeSnapshotCommitted.this) {
                            SimpleLabelTrie<SequentialNode> trie = SimpleLabelTrie.forRoot(SequentialNode.root());
                            ListenableFuture<Optional<SimpleLabelTrie<SequentialNode>>> future;
                            if (!error.isPresent()) {
                                final AbsoluteZNodePath root = (AbsoluteZNodePath) getWatchMatcher().getPath();
                                future = TreeWalker.forResult(new Sequentials(trie))
                                    .setRoot(root)
                                    .setRequests(TreeWalker.toRequests(TreeWalker.parameters().setSync(true)))
                                    .setIterator(GetEphemeralsIterator.create(root))
                                    .setClient(storage.materializer())
                                    .build();
                            } else {
                                future = Futures.immediateFuture(Optional.of(trie));
                            }
                            sequential = Optional.of(future);
                            sequential.get().addListener(sequentials, SameThreadExecutor.getInstance());
                            
                        }
                    }
                    if (error.isPresent()) {
                        stopping(state());
                    }
                    return error;
                }
            }
            
            protected class Sequentials implements Processor<Optional<? extends SubmittedRequest<Records.Request,?>>, Optional<SimpleLabelTrie<SequentialNode>>> {

                protected final SimpleLabelTrie<SequentialNode> trie;
                protected final PathToQuery<?,Message.ServerResponse<?>> query;
                
                @SuppressWarnings("unchecked")
                public Sequentials(SimpleLabelTrie<SequentialNode> trie) {
                    this.trie = trie;
                    this.query = PathToQuery.forRequests(storage.materializer(), Operations.Requests.sync(), Operations.Requests.exists().setWatch(true));
                    new SequentialDeleted().listen();
                }

                @Override
                public Optional<SimpleLabelTrie<SequentialNode>> apply(
                        Optional<? extends SubmittedRequest<Records.Request, ?>> input)
                        throws Exception {
                    if (input.isPresent()) {
                        SubmittedRequest<Records.Request, ?> request = input.get();
                        if (request.request() instanceof IGetChildrenRequest) {
                            if (((IGetChildrenRequest) request.request()).getPath().length() > root.length()) {
                                final Optional<Operation.Error> error = Operations.maybeError(request.get().record(), KeeperException.Code.NONODE);
                                if (!error.isPresent()) {
                                    synchronized (VolumeSnapshotCommitted.this) {
                                        final ZNodePath path = ZNodePath.fromString(((IGetChildrenRequest) request.request()).getPath());
                                        final Records.ChildrenGetter response = (Records.ChildrenGetter) request.get().record();
                                        SequentialNode parent = null;
                                        for (String child: response.getChildren()) {
                                            Optional<? extends Sequential<String, ?>> maybeSequential = Sequential.maybeFromString(child);
                                            if (maybeSequential.isPresent()) {
                                                if (parent == null) {
                                                    parent = SequentialNode.putIfAbsent(trie, path);
                                                }
                                                SequentialNode node = parent.putIfAbsent(ZNodeLabel.fromString(child));
                                                new Query((AbsoluteZNodePath) node.path());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return Optional.absent();
                    } else {
                        return Optional.of(trie);
                    }
                }
                
                public void remove(AbsoluteZNodePath path) {
                    synchronized (VolumeSnapshotCommitted.this) {
                        SequentialNode node = trie.get(path);
                        assert (node.isEmpty());
                        node.remove();
                        node = node.parent().get();
                        if (node.isEmpty()) {
                            while ((node != trie.root()) && node.isEmpty() && node.remove()) {
                                node = node.parent().get();
                            }
                        } else {
                            node = node.values().iterator().next();
                            if (node.get().isPresent()) {
                                sequentials.send(node.get().get());
                            }
                        }
                    }
                }
                
                protected class Query extends Watchers.Query {

                    protected final AbsoluteZNodePath path;
                    
                    public Query(AbsoluteZNodePath path) {
                        super(query.apply(path), SessionsWatcher.this);
                        this.path = path;
                    }

                    @Override
                    public Optional<Operation.Error> call() throws Exception {
                        final Optional<Operation.Error> error = super.call();
                        if (error.isPresent()) {
                            remove(path);
                        }
                        return error;
                    }
                }
                
                protected class SequentialDeleted extends AbstractWatchListener {

                    public SequentialDeleted() {
                        super(SessionsWatcher.this.service, 
                                SessionsWatcher.this.watch,
                                WatchMatcher.exact(
                                        SessionsWatcher.this.getWatchMatcher().getPath().join(ZNodeLabel.fromString(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.LABEL)).join(ZNodeLabel.fromString(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.Ephemeral.LABEL)), 
                                        Watcher.Event.EventType.NodeDeleted));
                    }

                    @Override
                    public void handleWatchEvent(WatchEvent event) {
                        remove((AbsoluteZNodePath) event.getPath());
                    }
                }
            }
        }

        protected abstract class ZNodeCreatedListener<E extends StorageZNode.EscapedNamedZNode<?>> extends CacheNodeCreatedListener<StorageZNode<?>> {
        
            protected ZNodeCreatedListener(
                    Class<? extends E> type) {
                super(storage.materializer().cache(), 
                        VolumeSnapshotCommitted.this.service, 
                        storage.cacheEvents(),
                        WatchMatcher.exact(
                                storage.materializer().schema().apply(type).path(), 
                                Watcher.Event.EventType.NodeDataChanged));
            }
        
            @SuppressWarnings("unchecked")
            @Override
            public void handleWatchEvent(WatchEvent event) {
                handleZNode((E) cache.cache().get(event.getPath()));
            }
            
            protected abstract void handleZNode(E node);
        }

        protected class EphemeralListener<V,E extends StorageZNode.EscapedNamedZNode<V>> extends ZNodeCreatedListener<StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.Ephemeral> {
        
            public EphemeralListener() {
                super(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.Ephemeral.class);
            }
        
            @Override
            protected void handleZNode(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.Ephemeral node) {
                ICreateRequest record;
                try {
                    record = (ICreateRequest) Records.Requests.deserialize(OpCode.CREATE, 
                            new ByteBufInputArchive(Unpooled.wrappedBuffer(node.data().get())));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                final Recreate recreate = new Recreate(
                        node.session().name().longValue(), 
                        (AbsoluteZNodePath) node.path(), 
                        record);
                try {
                    if (CreateMode.valueOf(record.getFlags()).contains(CreateFlag.SEQUENTIAL)) {
                        sequentials.send(recreate);
                    } else {
                        recreates.submit(recreate);
                    }
                } catch (RejectedExecutionException e) {}
            }
        }

        protected class WatchListener<V,E extends StorageZNode.EscapedNamedZNode<V>> extends ZNodeCreatedListener<StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.Session.Watch> {
        
            public WatchListener() {
                super(StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.Session.Watch.class);
            }
        
            @Override
            protected void handleZNode(StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.Session.Watch node) {
                final Watch watch = new Watch(node.session().name().longValue(), root.join(node.name()), node.data().get());
                try {
                    watches.send(Pair.create((AbsoluteZNodePath) node.path(), watch));
                } catch (RejectedExecutionException e) {}
            }
        }
        
        protected class SequentialProcessor extends Actors.QueuedActor<Recreate> {

            public SequentialProcessor() {
                super(Queues.<Recreate>newConcurrentLinkedQueue(), 
                        LogManager.getLogger(SequentialProcessor.class));
            }
            
            @Override
            public boolean isReady() {
                synchronized (VolumeSnapshotCommitted.this) {
                    return (!mailbox.isEmpty() && sequential.get().isDone());
                }
            }
            
            @Override
            protected boolean apply(Recreate recreate) throws Exception {
                synchronized (VolumeSnapshotCommitted.this) {
                    assert (sequential.get().isDone());
                    if (mailbox.remove(recreate)) {
                        SimpleLabelTrie<SequentialNode> trie = sequential.get().get().get();
                        SequentialNode node = trie.get(recreate.path());
                        if (!node.get().isPresent()) {
                            node.set(Optional.of(recreate));
                        } else {
                            assert (node.get().get() == recreate);
                        }
                        while (node.get().isPresent() && (node.parent().get().values().iterator().next() == node)) {
                            node.remove();
                            node = node.parent().get();
                            try {
                                recreates.submit(node.get().get());
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
        }
        
        protected class WatchProcessor extends Actors.QueuedActor<Pair<AbsoluteZNodePath, Watch>> {

            private final NoEphemerals noEphemerals;
            private final ImmutableMap<WatchType, Operations.PathBuilder<? extends Records.Request, ?>> requests;
            
            public WatchProcessor() {
                super(Queues.<Pair<AbsoluteZNodePath, Watch>>newConcurrentLinkedQueue(), 
                        LogManager.getLogger(WatchProcessor.class));
                this.requests = ImmutableMap.
                        <WatchType, Operations.PathBuilder<? extends Records.Request, ?>>of(WatchType.DATA, Operations.Requests.exists().setWatch(true));
                this.noEphemerals = new NoEphemerals();
                noEphemerals.listen();
            }
            
            @Override
            public boolean isReady() {
                return (!mailbox.isEmpty() && noEphemerals.isReady());
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
            
            protected class NoEphemerals extends AbstractWatchListener {

                protected NoEphemerals() {
                    super(VolumeSnapshotCommitted.this.service, 
                            storage.cacheEvents(), 
                            WatchMatcher.exact(
                                    ((AbsoluteZNodePath) VolumeSnapshotCommitted.this.getWatchMatcher().getPath()).parent().join(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.LABEL),
                                    Watcher.Event.EventType.NodeDeleted));
                }
                
                public boolean isReady() {
                    storage.materializer().cache().lock().readLock().lock();
                    try {
                        return !storage.materializer().cache().cache().containsKey(getWatchMatcher().getPath());
                    } finally {
                        storage.materializer().cache().lock().readLock().unlock();
                    }
                }

                @Override
                public void handleWatchEvent(WatchEvent event) {
                    WatchProcessor.this.run();
                }
            }
        }

        protected class RecreateProcessor extends ListenableFutureActor<Recreate, ShardedServerResponseMessage<?>, FutureValue<AbsoluteZNodePath,ShardedServerResponseMessage<?>>> {

            public RecreateProcessor() {
                super(Queues.<FutureValue<AbsoluteZNodePath,ShardedServerResponseMessage<?>>>newConcurrentLinkedQueue(), LogManager.getLogger(RecreateProcessor.class));
            }

            @Override
            public ListenableFuture<ShardedServerResponseMessage<?>> submit(
                    Recreate request) {
                BackendSessionExecutors.BackendSessionExecutor executor = executors.get(Long.valueOf(request.session()));
                if (executor != null) {
                    final ListenableFuture<ShardedServerResponseMessage<?>> future = executor.client().submit(
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
        
        protected class DeleteProcessor extends SubmitActor<IDeleteRequest,Message.ServerResponse<?>> {

            private final Operations.Requests.Delete delete;
            
            public DeleteProcessor() {
                super(storage.materializer(), 
                        Queues.<SubmittedRequest<IDeleteRequest,Message.ServerResponse<?>>>newConcurrentLinkedQueue(), 
                        LogManager.getLogger(DeleteProcessor.class));
                this.delete = Operations.Requests.delete();
            }
            
            public synchronized ListenableFuture<Message.ServerResponse<?>> submit(AbsoluteZNodePath path) {
                return submit(delete.setPath(path).build());
            }

            @Override
            protected boolean doApply(SubmittedRequest<IDeleteRequest,Message.ServerResponse<?>> input) throws Exception {
                super.doApply(input);
                storage.materializer().cache().lock().readLock().lock();
                try {
                    ZNodePath path = AbsoluteZNodePath.fromString(input.request().getPath()).parent();
                    StorageZNode<?> node = storage.materializer().cache().cache().get(path);
                    if ((node instanceof StorageZNode.SessionZNode) && node.isEmpty()) {
                        submit((AbsoluteZNodePath) node.path());
                    }
                } finally {
                    storage.materializer().cache().lock().readLock().unlock();
                }
                return true;
            }
        }
    }
    
    protected class SessionReconnectWatcher extends CacheNodeCreatedListener<StorageZNode<?>> {

        protected final PathToQuery<?,Message.ServerResponse<?>> query;
        
        @SuppressWarnings("unchecked")
        public SessionReconnectWatcher() {
            super(storage.materializer().cache(), 
                    SnapshotWatcher.this.service, 
                    storage.cacheEvents(), 
                    WatchMatcher.exact(
                            StorageSchema.Safari.Sessions.Session.PATH, 
                            Watcher.Event.EventType.NodeDataChanged));
            this.query = PathToQuery.forRequests(storage.materializer(), 
                    Operations.Requests.sync(), 
                    Operations.Requests.getChildren());
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            final StorageSchema.Safari.Sessions.Session session = (StorageSchema.Safari.Sessions.Session) cache.cache().get(event.getPath());
            if ((session.stat().stamp() > 0L) && (session.stat().get().getVersion() > 1)) {
                if (executors.get(Long.valueOf(session.name().longValue())) != null) {
                    final ImmutableList.Builder<AbsoluteZNodePath> builder = ImmutableList.builder();
                    for (StorageZNode<?> volume: cache.cache().get(StorageSchema.Safari.Volumes.PATH).values()) {
                        if (volume.size() == volume.schema().size()) {
                            for (StorageZNode<?> child: volume.values()) {
                                StorageZNode<?> node = child.get(session.parent().name());
                                if (node != null) {
                                    builder.add((AbsoluteZNodePath) node.path());
                                }
                            }    
                        }
                    }
                    for (AbsoluteZNodePath path: builder.build()) {
                        Watchers.Query.call(query.apply(path), this);
                    }
                }
            }
        }
    }

    protected static class QueryIfPredicate<E extends StorageZNode<?>> extends CacheNodeCreatedListener<StorageZNode<?>> {
    
        public static <E extends StorageZNode<?>> QueryIfPredicate<E> listen(
                Predicate<? super E> doQuery,
                Class<? extends E> type,
                Materializer<StorageZNode<?>,?> materializer,
                Service service, 
                WatchListeners watch,
                Operations.Builder<? extends Records.Request>... builders) {
            QueryIfPredicate<E> instance = new QueryIfPredicate<E>(doQuery, type, materializer, service, watch, builders);
            instance.listen();
            return instance;
        }
        
        protected final Predicate<? super E> doQuery;
        protected final PathToQuery<?,?> query;
        
        protected QueryIfPredicate(
                Predicate<? super E> doQuery,
                Class<? extends E> type,
                Materializer<StorageZNode<?>,?> materializer,
                Service service, 
                WatchListeners watch,
                Operations.Builder<? extends Records.Request>... builders) {
            super(materializer.schema().apply(type).path(), 
                    materializer.cache(), 
                    service, 
                    watch);
            this.doQuery = doQuery;
            this.query = PathToQuery.forRequests(
                    materializer, 
                    builders);
        }
    
        @Override
        public void handleWatchEvent(WatchEvent event) {
            @SuppressWarnings("unchecked")
            final E node = (E) cache.cache().get(event.getPath());
            if (doQuery.apply(node)) {
                newQuery(event.getPath());
            }
        }
        
        protected Watchers.Query newQuery(ZNodePath path) {
            return Watchers.Query.call(query.apply(path), this);
        }
    }

    protected static final class GetEphemeralsIterator implements Processor<SubmittedRequest<Records.Request,?>, Iterator<AbsoluteZNodePath>> {
    
        public static GetEphemeralsIterator create(
                AbsoluteZNodePath root) {
            return new GetEphemeralsIterator(root);
        }
        
        protected final AbsoluteZNodePath root;
        protected final TreeWalker.GetChildrenIterator delegate;
        
        protected GetEphemeralsIterator(
                AbsoluteZNodePath root) {
            this.root = root;
            this.delegate = TreeWalker.GetChildrenIterator.create();
        }
        
        @Override
        public Iterator<AbsoluteZNodePath> apply(SubmittedRequest<Records.Request,?> input) throws InterruptedException, ExecutionException {
            if (input.request() instanceof IGetChildrenRequest) {
                final String path = ((IGetChildrenRequest) input.request()).getPath();
                if (path.length() > root.length()) {
                    return Iterators.emptyIterator();
                }
            }
            return delegate.apply(input);
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
        protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper helper) {
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
    
    protected static final class SequentialNode extends DefaultsNode.AbstractDefaultsNode<SequentialNode> implements Supplier<Optional<Recreate>> {

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
        
        public static SequentialNode root() {
            return new SequentialNode(AbstractNameTrie.<SequentialNode>rootPointer());
        }
        
        public static SequentialNode childOf(ZNodeName label, SequentialNode parent) {
            return new SequentialNode(AbstractNameTrie.<SequentialNode>weakPointer(label, parent));
        }
        
        private Optional<Recreate> recreate;

        protected SequentialNode(Pointer<? extends SequentialNode> parent) {
            super(parent, Maps.<ZNodeName, ZNodeName, SequentialNode>newTreeMap(COMPARATOR));
            this.recreate = Optional.absent();
        }

        @Override
        protected SequentialNode newChild(ZNodeName label) {
            return childOf(label, this);
        }
        
        @Override
        public Optional<Recreate> get() {
            return recreate;
        }
       
        public void set(Optional<Recreate> recreate) {
            this.recreate = recreate;
        }
    }
}
