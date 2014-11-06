package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.TreeWalker;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Aborts snapshot state for closed sessions.
 * 
 * Assumes that session state and snapshot state are already watched.
 */
public final class AbortClosedSessionSnapshots extends WatchMatchServiceListener {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public AbortClosedSessionSnapshots getAbortClosedSessionSnapshots(
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> client,
                final RegionRoleService service) {
            return AbortClosedSessionSnapshots.listen(
                    client.materializer(), 
                    client.cacheEvents(),
                    service);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(AbortClosedSessionSnapshots.class);
        }

        @Override
        protected void configure() {
        }
    }
    
    public static AbortClosedSessionSnapshots listen(
            Materializer<StorageZNode<?>,?> materializer,
            WatchListeners cacheEvents,
            Service service) {
        final Set<AbsoluteZNodePath> snapshots = Collections.synchronizedSet(
                Sets.<AbsoluteZNodePath>newHashSet());
        final Logger logger = LogManager.getLogger(AbortClosedSessionSnapshots.class);
        final AbortSessionSnapshotCallback<Boolean> callback = AbortSessionSnapshotCallback.create(snapshots, materializer, Watchers.StopServiceOnFailure.create(service), logger);
        ClosedSessionsSnapshotWatcher.listen(snapshots, callback, materializer, cacheEvents, service, logger);
        final AbortClosedSessionSnapshots instance = new AbortClosedSessionSnapshots(
                snapshots,
                cacheEvents, 
                service,
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(callback), 
                        WatchMatcher.exact(
                                StorageSchema.Safari.Sessions.Session.PATH, 
                                Watcher.Event.EventType.NodeDeleted),
                        logger));
        instance.listen();
        return instance;
    }

    protected final Set<AbsoluteZNodePath> snapshots;
    
    protected AbortClosedSessionSnapshots(
            Set<AbsoluteZNodePath> snapshots,
            WatchListeners cacheEvents,
            Service service,
            WatchMatchListener listener) {
        super(service, cacheEvents, listener);
        this.snapshots = snapshots;
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        snapshots.clear();
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        snapshots.clear();
        Services.stop(service);
    }
    
    protected static final class AbortSessionSnapshotCallback<V> extends Watchers.SimpleForwardingCallback<ZNodePath, FutureCallback<? super V>> implements AsyncFunction<ZNodePath, V> {

        public static AbortSessionSnapshotCallback<Boolean> create(
                Set<AbsoluteZNodePath> snapshots,
                Materializer<StorageZNode<?>,?> materializer,
                FutureCallback<? super Boolean> callback,
                Logger logger) {
            final AbortSessionSnapshot abort = AbortSessionSnapshot.create(materializer);
            return create(snapshots, abort, materializer.cache().cache(), callback, logger);
        }

        public static <V> AbortSessionSnapshotCallback<V> create(
                Set<AbsoluteZNodePath> snapshots,
                AsyncFunction<? super ZNodePath, V> abort,
                NameTrie<StorageZNode<?>> trie,
                FutureCallback<? super V> callback,
                Logger logger) {
            return new AbortSessionSnapshotCallback<V>(snapshots, abort, trie, callback, logger);
        }
        
        private final Set<AbsoluteZNodePath> snapshots;
        private final AsyncFunction<? super ZNodePath, V> abort;
        private final NameTrie<StorageZNode<?>> trie;
        private final Logger logger;
        
        protected AbortSessionSnapshotCallback(
                Set<AbsoluteZNodePath> snapshots,
                AsyncFunction<? super ZNodePath, V> abort,
                NameTrie<StorageZNode<?>> trie,
                FutureCallback<? super V> delegate,
                Logger logger) {
            super(delegate);
            this.snapshots = snapshots;
            this.abort = abort;
            this.trie = trie;
            this.logger = logger;
        }

        @Override
        public ListenableFuture<V> apply(ZNodePath input) {
            logger.debug("Aborting session snapshot {}", input);
            ListenableFuture<V> future;
            try {
                future = abort.apply(input);
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            Futures.addCallback(future, delegate());
            return future;
        }

        @Override
        public void onSuccess(ZNodePath result) {
            ImmutableList<AbsoluteZNodePath> toAbort = ImmutableList.of();
            final ZNodeLabel label = (ZNodeLabel) result.label();
            synchronized (snapshots) {
                for (AbsoluteZNodePath path: snapshots) {
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = 
                            (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) trie.get(path);
                    // only abort committed snapshot state
                    if ((snapshot != null) && (snapshot.commit() != null) && (Objects.equal(snapshot.commit().data().get(), Boolean.TRUE))) {
                        for (StorageZNode<?> node: snapshot.values()) {
                            StorageZNode<?> snapshotSessions = node.get(StorageZNode.SessionsZNode.LABEL);
                            if (snapshotSessions != null) {
                                StorageZNode<?> session = snapshotSessions.get(label);
                                if ((session != null) && !session.containsKey(StorageZNode.CommitZNode.LABEL)) {
                                    toAbort = ImmutableList.<AbsoluteZNodePath>builder()
                                    .addAll(toAbort)
                                    .add((AbsoluteZNodePath) session.path()).build();
                                }
                            }
                        }
                    }
                }
            }
            for (AbsoluteZNodePath path: toAbort) {
                apply(path);
            }
        }
    }
    
    protected static final class AbortSessionSnapshot implements AsyncFunction<ZNodePath, Boolean> {
        
        public static AbortSessionSnapshot create(
                Materializer<StorageZNode<?>,?> materializer) {
            return new AbortSessionSnapshot(materializer);
        }
        
        private final Materializer<StorageZNode<?>,?> materializer;
        private final AsyncFunction<ZNodePath, Optional<Deque<AbsoluteZNodePath>>> getUncommitted;
        
        protected AbortSessionSnapshot(
                final Materializer<StorageZNode<?>,?> materializer) {
            this.materializer = materializer;
            this.getUncommitted = new AsyncFunction<ZNodePath, Optional<Deque<AbsoluteZNodePath>>>() {
                final TreeWalker.Builder<?> walker = TreeWalker.builder().setIterator(UncommittedIterator.create(materializer.schema().get())).setClient(materializer);
                @Override
                public ListenableFuture<Optional<Deque<AbsoluteZNodePath>>> apply(
                        ZNodePath input) throws Exception {
                    return walker.setRoot(input).setResult(UncommittedStackWalker.create(materializer.schema().get())).build();
                }
            };
        }
        
        public ListenableFuture<Boolean> apply(ZNodePath input) throws Exception {
            return Futures.transform(getUncommitted.apply(input), new Callback());
        }
        
        private final class Callback implements AsyncFunction<Object, Boolean> {
            private final Operations.Requests.Create create;
            
            protected Callback() {
                try {
                    this.create = Operations.Requests.create().setData(materializer.codec().toBytes(Boolean.FALSE));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            
            @SuppressWarnings("unchecked")
            public ListenableFuture<Boolean> apply(Object input) throws Exception {
                if (input instanceof Optional<?>) {
                    ImmutableList.Builder<ListenableFuture<? extends Operation.ProtocolResponse<?>>> futures = ImmutableList.builder();
                    Deque<AbsoluteZNodePath> paths = (Deque<AbsoluteZNodePath>) ((Optional<?>) input).get();
                    while (!paths.isEmpty()) {
                        futures.add(materializer.submit(create.setPath(paths.removeFirst()).build()));
                    }
                    return Futures.transform(Futures.allAsList(futures.build()), this);
                } else {
                    for (Operation.ProtocolResponse<?> response: (List<? extends Operation.ProtocolResponse<?>>) input) {
                        Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS, KeeperException.Code.NONODE);
                    }
                    return Futures.immediateFuture(Boolean.TRUE);
                }
            }
        }
    }
    
    protected static final class UncommittedStackWalker implements Processor<Optional<? extends SubmittedRequest<Records.Request,?>>, Optional<Deque<AbsoluteZNodePath>>> {

        public static UncommittedStackWalker create(NameTrie<ValueNode<ZNodeSchema>> schema) {
            return new UncommittedStackWalker(schema, Queues.<AbsoluteZNodePath>newArrayDeque());
        }
        
        private final NameTrie<ValueNode<ZNodeSchema>> schema;
        private final Deque<AbsoluteZNodePath> uncommitted;
        
        protected UncommittedStackWalker(
                NameTrie<ValueNode<ZNodeSchema>> schema,
                Deque<AbsoluteZNodePath> toAbort) {
            this.schema = schema;
            this.uncommitted = toAbort;
        }
        
        @Override
        public Optional<Deque<AbsoluteZNodePath>> apply(
                Optional<? extends SubmittedRequest<Records.Request, ?>> input)
                throws Exception {
            Optional<Deque<AbsoluteZNodePath>> result = Optional.absent();
            if (input.isPresent()) {
                final Records.Response response = input.get().get().record();
                if (response instanceof Records.ChildrenGetter) {
                    ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.get().getValue()).getPath());
                    ValueNode<ZNodeSchema> schema = ZNodeSchema.matchPath(this.schema, path);
                    if (schema.get(StorageZNode.CommitZNode.LABEL) != null) {
                        if (! ((Records.ChildrenGetter) response).getChildren().contains(StorageZNode.CommitZNode.LABEL.toString())) {
                            uncommitted.addFirst((AbsoluteZNodePath) path.join(StorageZNode.CommitZNode.LABEL));
                        }
                    }
                }
            } else {
                result = Optional.of(uncommitted);
            }
            return result;
        }
    }
    
    protected static final class UncommittedIterator implements Processor<SubmittedRequest<Records.Request,?>, Iterator<AbsoluteZNodePath>> {

        public static UncommittedIterator create(
                NameTrie<ValueNode<ZNodeSchema>> schema) {
            return new UncommittedIterator(schema, HasChildrenFilter.create(schema));
        }
        
        private final NameTrie<ValueNode<ZNodeSchema>> schema;
        private final Processor<SubmittedRequest<Records.Request,?>, Iterator<AbsoluteZNodePath>> delegate;
        
        protected UncommittedIterator(
                NameTrie<ValueNode<ZNodeSchema>> schema,
                Processor<SubmittedRequest<Records.Request,?>, Iterator<AbsoluteZNodePath>> delegate) {
            this.schema = schema;
            this.delegate = delegate;
        }
        
        @Override
        public Iterator<AbsoluteZNodePath> apply(
                SubmittedRequest<Records.Request, ?> input)
                throws Exception {
            final Records.Response response = input.get().record();
            if (response instanceof Records.ChildrenGetter) {
                ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.getValue()).getPath());
                ValueNode<ZNodeSchema> schema = ZNodeSchema.matchPath(this.schema, path);
                if ((schema.get(StorageZNode.CommitZNode.LABEL) != null) && ((Records.ChildrenGetter) response).getChildren().contains(StorageZNode.CommitZNode.LABEL.toString())) {
                    return ImmutableSet.<AbsoluteZNodePath>of().iterator();
                }
            }
            return delegate.apply(input);
        }

        protected static final class FilteredIterator<I,O> implements Processor<I, Iterator<O>> {
        
            public static <I,O> FilteredIterator<I,O> create(
                    Processor<? super I, Iterator<O>> delegate,
                    Predicate<O> filter) {
                return new FilteredIterator<I,O>(delegate, filter);
            }
            
            private final Processor<? super I, Iterator<O>> delegate;
            private final Predicate<? super O> filter;
            
            protected FilteredIterator(
                    Processor<? super I, Iterator<O>> delegate,
                    Predicate<O> filter) {
                this.delegate = delegate;
                this.filter = filter;
            }
            
            @Override
            public Iterator<O> apply(I input) throws Exception {
                return Iterators.filter(delegate.apply(input), filter);
            }
        }
        
        protected static final class HasChildrenFilter implements Predicate<AbsoluteZNodePath> {

            public static FilteredIterator<SubmittedRequest<Records.Request,?>,AbsoluteZNodePath> create(
                    NameTrie<ValueNode<ZNodeSchema>> schema) {
                return FilteredIterator.create(TreeWalker.GetChildrenIterator.create(), new HasChildrenFilter(schema));
            }

            private final NameTrie<ValueNode<ZNodeSchema>> schema;
            
            protected HasChildrenFilter(
                    NameTrie<ValueNode<ZNodeSchema>> schema) {
                this.schema = schema;
            }
            
            @Override
            public boolean apply(AbsoluteZNodePath input) {
                ValueNode<ZNodeSchema> node = ZNodeSchema.matchPath(schema, input);
                return !node.isEmpty();
            }
        }
    }
    
    /**
     * Tracks committed snapshots and checks for expired sessions when snapshot state is committed.
     */
    protected static final class ClosedSessionsSnapshotWatcher extends LoggingWatchMatchListener {
        
        public static Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
                Set<AbsoluteZNodePath> snapshots,
                AbortSessionSnapshotCallback<?> callback,
                Materializer<StorageZNode<?>,?> materializer,
                WatchListeners cacheEvents,
                Service service,
                Logger logger) {
            return Watchers.CacheNodeCreatedListener.listen(
                    materializer.cache(),
                    service, 
                    cacheEvents,
                    new ClosedSessionsSnapshotWatcher(snapshots, callback, materializer, logger),
                    logger);
        }

        private final NameTrie<StorageZNode<?>> trie;
        private final Set<AbsoluteZNodePath> snapshots;
        private final PathToQuery<?,?> query;
        private final Watchers.MaybeErrorProcessor processor;
        private final AbortSessionSnapshotCallback<?> callback;
        
        @SuppressWarnings("unchecked")
        protected ClosedSessionsSnapshotWatcher(
                Set<AbsoluteZNodePath> snapshots,
                AbortSessionSnapshotCallback<?> callback,
                Materializer<StorageZNode<?>,?> materializer,
                Logger logger) {
            super(WatchMatcher.exact(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH, 
                    Watcher.Event.EventType.NodeDataChanged, 
                    Watcher.Event.EventType.NodeDeleted), 
                logger);
            this.trie = materializer.cache().cache();
            this.snapshots = snapshots;
            this.callback = callback;
            this.query = PathToQuery.forRequests(
                    materializer, 
                    Operations.Requests.sync(), 
                    Operations.Requests.exists());
            this.processor = Watchers.MaybeErrorProcessor.maybeNoNode();
        }
        
        public Set<AbsoluteZNodePath> getSnapshots() {
            return snapshots;
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            switch (event.getEventType()) {
            case NodeDataChanged:
            {
                StorageZNode<?> commit = trie.get(event.getPath());
                if ((commit != null) && Objects.equal(commit.data().get(), Boolean.TRUE)) {
                    StorageZNode<?> snapshot = commit.parent().get();
                    StorageZNode<?> sessions = trie.get(StorageSchema.Safari.Sessions.PATH);
                    snapshots.add((AbsoluteZNodePath) snapshot.path());
                    for (StorageZNode<?> node: snapshot.values()) {
                        StorageZNode<?> snapshotSessions = node.get(StorageZNode.SessionsZNode.LABEL);
                        if (snapshotSessions != null) {
                            for (StorageZNode<?> session: snapshotSessions.values()) {
                                if (!session.containsKey(StorageZNode.CommitZNode.LABEL) && !sessions.containsKey(session.parent().name())) {
                                    Watchers.Query.call(
                                            processor, 
                                            new Callback((AbsoluteZNodePath) session.path()), 
                                            query.apply(sessions.path().join(session.parent().name()))).run();
                                    
                                }
                            }
                        }
                    }
                }
                break;
            }
            case NodeDeleted:
            {
                final AbsoluteZNodePath snapshot = (AbsoluteZNodePath) ((AbsoluteZNodePath) event.getPath()).parent();
                snapshots.remove(snapshot);
                break;
            }
            default:
                break;
            }
        }
        
        protected final class Callback extends Watchers.ForwardingCallback<Optional<Operation.Error>, FutureCallback<?>> {

            private final AbsoluteZNodePath path;
            
            protected Callback(AbsoluteZNodePath path) {
                this.path = path;
            }
            
            @Override
            public void onSuccess(Optional<Operation.Error> result) {
                if (result.isPresent()) {
                    callback.apply(path);
                }
            }

            @Override
            protected FutureCallback<?> delegate() {
                return callback;
            }
        }
    }
}
