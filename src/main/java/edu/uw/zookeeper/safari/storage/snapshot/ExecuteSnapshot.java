package edu.uw.zookeeper.safari.storage.snapshot;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.Unpooled;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.TreeWalker;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.ListenableFutureActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.SubmitActor;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.common.ValueFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.SimpleNameTrie;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterWords;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.proto.ByteBufOutputArchive;
import edu.uw.zookeeper.protocol.proto.ICreateRequest;
import edu.uw.zookeeper.protocol.proto.ICreateResponse;
import edu.uw.zookeeper.protocol.proto.IDeleteRequest;
import edu.uw.zookeeper.protocol.proto.IGetDataRequest;
import edu.uw.zookeeper.protocol.proto.IStat;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Stats;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.backend.PrefixTranslator;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;


public final class ExecuteSnapshot<O extends Operation.ProtocolResponse<?>> implements ChainedFutures.ChainedProcessor<ExecuteSnapshot.Action<?>, ChainedFutures.DequeChain<ExecuteSnapshot.Action<?>,?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton @Snapshot
        public AsyncFunction<Boolean,Long> getExecuteSnapshot(
                final @From ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> fromClient,
                final @From SnapshotVolumeParameters fromVolume,
                final @To ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> toClient,
                final @To SnapshotVolumeParameters toVolume,
                final @From EnsembleView<ServerInetAddressView> fromEnsemble,
                final @Storage Materializer<StorageZNode<?>,?> fromMaterializer,
                final @From ServerInetAddressView fromAddress,
                final @Snapshot AsyncFunction<Long, Long> sessions,
                final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
            return new AsyncFunction<Boolean, Long>() {
                @Override
                public ListenableFuture<Long> apply(Boolean input) throws Exception {
                    if (input.booleanValue()) {
                        return ExecuteSnapshot.recover(
                                fromClient, 
                                fromVolume, 
                                toClient, 
                                toVolume,
                                fromMaterializer, 
                                fromAddress, 
                                fromEnsemble,
                                sessions, 
                                anonymous);
                    } else {
                        return Futures.transform(
                                undo(toVolume, toClient),
                                Functions.constant(Long.valueOf(0L)));
                    }
                }
            };
        }

        @Override
        public Key<?> getKey() {
            return Key.get(new TypeLiteral<Callable<ListenableFuture<Boolean>>>(){});
        }

        @Override
        protected void configure() {
        }
    }
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Long> recover(
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient,
            SnapshotVolumeParameters fromVolume,
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient,
            SnapshotVolumeParameters toVolume,
            Materializer<StorageZNode<?>,?> materializer,
            ServerInetAddressView server,
            EnsembleView<ServerInetAddressView> storage,
            AsyncFunction<Long, Long> sessions,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
        Deque<ExecuteSnapshot.Action<?>> steps = Queues.newArrayDeque(ImmutableList.<Action<?>>of(undo(toVolume, toClient)));
        return create(
                fromClient, 
                fromVolume,
                toClient, 
                toVolume,
                materializer, 
                server,
                storage,
                sessions,
                anonymous,
                steps);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> DeleteExistingSnapshot undo(
            SnapshotVolumeParameters toVolume,
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient) {
        return DeleteExistingSnapshot.create(toVolume, toClient);
    }

    protected static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Long> create(
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient,
            SnapshotVolumeParameters fromVolume,
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient,
            SnapshotVolumeParameters toVolume,
            Materializer<StorageZNode<?>,?> materializer,
            ServerInetAddressView server,
            EnsembleView<ServerInetAddressView> storage,
            AsyncFunction<Long, Long> sessions,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous,
            Deque<ExecuteSnapshot.Action<?>> steps) {
        final AbsoluteZNodePath fromRoot = StorageSchema.Safari.Volumes.Volume.Root.pathOf(fromVolume.getVolume()).join(fromVolume.getBranch());
        final AbsoluteZNodePath toRoot = StorageSchema.Safari.Volumes.Volume.Root.pathOf(toVolume.getVolume()).join(toVolume.getBranch());
        final PathTranslator paths = PathTranslator.empty(fromRoot, toRoot);
        final ZxidTracker zxid = ZxidTracker.zero();
        final ExecuteSnapshot<O> snapshot = new ExecuteSnapshot<O>(
                fromClient, 
                fromVolume, 
                fromRoot, 
                toClient, 
                toVolume,
                toRoot, 
                materializer, 
                server,
                storage,
                anonymous,
                paths, 
                sessions,
                zxid);
        return ChainedFutures.run(
                ChainedFutures.result(
                        new Processor<FutureChain.FutureDequeChain<ExecuteSnapshot.Action<?>>, Long>() {
                            @Override
                            public Long apply(
                                    FutureChain.FutureDequeChain<ExecuteSnapshot.Action<?>> input)
                                    throws Exception {
                                Iterator<ExecuteSnapshot.Action<?>> actions = input.descendingIterator();
                                while (actions.hasNext()) {
                                    Object result = actions.next().get();
                                    if (result instanceof Long) {
                                        return (Long) result;
                                    }
                                }
                                throw new AssertionError();
                            }
                        },
                        ChainedFutures.deque(
                                snapshot, 
                                steps)));
    }
    
    protected final ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient;
    protected final SnapshotVolumeParameters fromVolume;
    protected final ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient;
    protected final SnapshotVolumeParameters toVolume;
    protected final Materializer<StorageZNode<?>,?> materializer;
    protected final ServerInetAddressView server;
    protected final EnsembleView<ServerInetAddressView> storage;
    protected final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous;
    protected final AbsoluteZNodePath fromRoot;
    protected final AbsoluteZNodePath toRoot;
    protected final AsyncFunction<Long, Long> sessions;
    protected final PathTranslator paths;
    protected final WalkerProcessor walker;
    
    protected ExecuteSnapshot(
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient,
            SnapshotVolumeParameters fromVolume,
            AbsoluteZNodePath fromRoot,
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient,
            SnapshotVolumeParameters toVolume,
            AbsoluteZNodePath toRoot,
            Materializer<StorageZNode<?>,?> materializer,
            ServerInetAddressView server,
            EnsembleView<ServerInetAddressView> storage,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous,
            PathTranslator paths,
            AsyncFunction<Long, Long> sessions,
            ZxidTracker fromZxid) {
        this.fromClient = fromClient;
        this.fromVolume = fromVolume;
        this.toClient = toClient;
        this.toVolume = toVolume;
        this.materializer = materializer;
        this.server = server;
        this.storage = storage;
        this.anonymous = anonymous;
        this.fromRoot = fromRoot;
        this.toRoot = toRoot;
        this.paths = paths;
        this.sessions = sessions;
        this.walker = new WalkerProcessor(fromZxid,
                SettableFuturePromise.<Long>create());
    }
    
    @Override
    public Optional<? extends Action<?>> apply(ChainedFutures.DequeChain<Action<?>,?> input) {
        Optional<? extends ListenableFuture<?>> last = input.isEmpty() ? Optional.<ListenableFuture<?>>absent() : Optional.of(input.getLast());
        if (last.isPresent()) {
            try {
                last.get().get();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
        }
        if (!last.isPresent() || (last.get() instanceof DeleteExistingSnapshot)) {
            return Optional.of(CreateSnapshotPrefix.create(toVolume.getVolume(), toVolume.getVersion(), toClient));
        } else if (last.get() instanceof CreateSnapshotPrefix) {
            return Optional.of(
                    PrepareSnapshotEnsembleWatches.create(
                            StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.pathOf(toVolume.getVolume(), toVolume.getVersion()),
                        new Function<ZNodePath,ZNodeLabel>() {
                            @Override
                            public ZNodeLabel apply(ZNodePath input) {
                                return (ZNodeLabel) StorageZNode.EscapedNamedZNode.converter().convert(
                                                paths.apply(input).suffix(toRoot));
                            }
                        },
                        server,
                        Futures.transform(
                                fromClient.session(), 
                                new Function<ConnectMessage.Response,Long>() {
                                    @Override
                                    public Long apply(ConnectMessage.Response input) {
                                        return Long.valueOf(input.getSessionId());
                                    }
                                }),
                        materializer,
                        toClient,
                        storage,
                        anonymous,
                        sessions,
                        new SnapshotWatches.FilteredWchc(
                                new FromRootWchc(fromRoot))));
        } else if (last.get() instanceof PrepareSnapshotEnsembleWatches) {
            try {
                return Optional.of((CallSnapshotEnsembleWatches)
                        CallSnapshotEnsembleWatches.create(
                                ((PrepareSnapshotEnsembleWatches) last.get()).get()));
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
        } else if (last.get() instanceof CallSnapshotEnsembleWatches) {
            Action<?> previous = Iterators.get(input.descendingIterator(), 1);
            if (previous instanceof PrepareSnapshotEnsembleWatches) {
                final ListenableFuture<Long> future = Futures.transform(
                        walker.walk(fromRoot), 
                        new GetOptional<Long>());
                return Optional.of(CreateSnapshot.call(future));
            } else if (previous instanceof CreateSnapshot) {
                return Optional.absent();
            } else {
                throw new AssertionError();
            }
        } else if (last.get() instanceof CreateSnapshot) {
            CallSnapshotEnsembleWatches watches = (CallSnapshotEnsembleWatches) Iterators.get(input.descendingIterator(), 1);
            return Optional.of((CallSnapshotEnsembleWatches) CallSnapshotEnsembleWatches.create(watches.chain()));
        }
        return Optional.absent();
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("from", fromVolume).add("to", toVolume).toString();
    }
    
    public static final class FromRootWchc implements Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, Collection<ZNodePath>> {

        private final ZNodePath root;
        
        public FromRootWchc(ZNodePath root) {
            this.root = root;
        }
        
        @Override
        public Collection<ZNodePath> apply(Map.Entry<Long, ? extends Collection<ZNodePath>> entry) {
            ImmutableList.Builder<ZNodePath> filtered = ImmutableList.builder();
            for (ZNodePath path: entry.getValue()) {
                if (path.startsWith(root)) {
                    filtered.add(path);
                }
            }
            return filtered.build();
        }
    }
    
    public static final class WithoutSessionsWchc implements Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, Collection<ZNodePath>> {

        private final ImmutableSet<Long> sessions;
        
        public WithoutSessionsWchc(ImmutableSet<Long> sessions) {
            this.sessions = sessions;
        }
        
        @Override
        public Collection<ZNodePath> apply(Map.Entry<Long, ? extends Collection<ZNodePath>> entry) {
            return sessions.contains(entry.getKey()) ? 
                    ImmutableSet.<ZNodePath>of() : 
                        entry.getValue();
        }
    }
    
    protected static final class GetOptional<V> implements AsyncFunction<Optional<ListenableFuture<V>>, V> {

        public GetOptional() {}
        
        @Override
        public ListenableFuture<V> apply(
                Optional<ListenableFuture<V>> input)
                throws Exception {
            return input.get();
        }
    }
    
    public static abstract class Action<V> extends SimpleToStringListenableFuture<V> {

        protected Action(ListenableFuture<V> delegate) {
            super(delegate);
        }
    }
    
    public static final class DeleteExistingSnapshot extends Action<Boolean> {

        public static DeleteExistingSnapshot create(
                final SnapshotVolumeParameters toVolume,
                final ClientExecutor<? super Records.Request,?,?> client) {
            ImmutableList<AbsoluteZNodePath> paths = 
                    ImmutableList.of(
                            StorageSchema.Safari.Volumes.Volume.Root.pathOf(toVolume.getVolume()).join(toVolume.getBranch()),
                            StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.pathOf(toVolume.getVolume(), toVolume.getVersion()),
                            StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.pathOf(toVolume.getVolume(), toVolume.getVersion()));
            return new DeleteExistingSnapshot(Deleted.create(paths, client));
        }
        
        protected DeleteExistingSnapshot(
                ListenableFuture<Boolean> future) {
            super(future);
        }

        protected static class Deleted extends SimpleToStringListenableFuture<List<List<AbsoluteZNodePath>>> implements Callable<Optional<Boolean>> {

            public static ListenableFuture<Boolean> create(
                    List<? extends ZNodePath> paths,
                    ClientExecutor<? super Records.Request,?,?> client) {
                ImmutableList.Builder<ListenableFuture<List<AbsoluteZNodePath>>> deletes = 
                        ImmutableList.builder();
                for (ZNodePath path: paths) {
                    deletes.add(DeleteSubtree.deleteChildren(path, client));
                }
                CallablePromiseTask<Deleted,Boolean> task = CallablePromiseTask.listen(
                        new Deleted(Futures.allAsList(deletes.build())), 
                        SettableFuturePromise.<Boolean>create());
                return task;
            }
            
            protected Deleted(ListenableFuture<List<List<AbsoluteZNodePath>>> future) {
                super(future);
            }
            
            @Override
            public Optional<Boolean> call() throws Exception {
                if (isDone()) {
                    Boolean deleted = Boolean.FALSE;
                    for (List<AbsoluteZNodePath> paths: get()) {
                        if (!paths.isEmpty() && !deleted.booleanValue()) {
                            deleted = Boolean.TRUE;
                        }
                    }
                    return Optional.of(deleted);
                }
                return Optional.absent();
            }
        }
    }

    /**
     * Assumes that version, root, and toBranch is already created.
     */
    public static final class CreateSnapshotPrefix extends Action<Boolean> {
        
        public static <O extends Operation.ProtocolResponse<?>> CreateSnapshotPrefix create(
                Identifier toVolume,
                UnsignedLong toVersion,
                ClientExecutor<? super Records.Request,O,?> client) {
            final AbsoluteZNodePath snapshotPath = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.pathOf(toVolume, toVersion);
            ImmutableList<AbsoluteZNodePath> paths = 
                    ImmutableList.of(
                            snapshotPath,
                            snapshotPath.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL),
                            snapshotPath.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.LABEL));
            return new CreateSnapshotPrefix(Created.create(paths, client));
        }
        
        protected CreateSnapshotPrefix(ListenableFuture<Boolean> future) {
            super(future);
        }

        protected static class Created<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> implements Callable<Optional<Boolean>> {

            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Boolean> create(
                    List<? extends ZNodePath> paths,
                    ClientExecutor<? super Records.Request,O,?> client) {
                ImmutableList.Builder<Records.Request> creates = 
                        ImmutableList.builder();
                for (ZNodePath path: paths) {
                    creates.add(Operations.Requests.create().setPath(path).build());
                }
                CallablePromiseTask<Created<O>,Boolean> task = CallablePromiseTask.create(new Created<O>(
                        SubmittedRequests.submit(client, creates.build())), 
                        SettableFuturePromise.<Boolean>create());
                task.task().addListener(task, MoreExecutors.directExecutor());
                return task;
            }
            
            protected Created(ListenableFuture<List<O>> future) {
                super(future);
            }
            
            @Override
            public Optional<Boolean> call() throws Exception {
                if (isDone()) {
                    Boolean created = Boolean.FALSE;
                    for (O response: get()) {
                        if (!Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS).isPresent() && !created.booleanValue()) {
                            created = Boolean.TRUE;
                        }
                    }
                    return Optional.of(created);
                }
                return Optional.absent();
            }
        }
    }
    
    public static final class PrepareSnapshotEnsembleWatches extends Action<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>> {
        
        public static <O extends Operation.ProtocolResponse<?>> PrepareSnapshotEnsembleWatches create(
                final ZNodePath prefix,
                final Function<ZNodePath,ZNodeLabel> labelOf,
                final ServerInetAddressView server,
                final ListenableFuture<Long> session,
                final Materializer<StorageZNode<?>,?> materializer,
                final ClientExecutor<? super Records.Request, O, SessionListener> client,
                final EnsembleView<ServerInetAddressView> ensemble,
                final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous,
                final AsyncFunction<Long, Long> sessions,
                final Function<FourLetterWords.Wchc, FourLetterWords.Wchc> transformer) {
            final ListenableFuture<Function<FourLetterWords.Wchc, FourLetterWords.Wchc>> localTransformer = 
                    Futures.transform(
                            session, 
                            new Function<Long,Function<FourLetterWords.Wchc, FourLetterWords.Wchc>>() {
                                @Override
                                public Function<FourLetterWords.Wchc, FourLetterWords.Wchc> apply(Long input) {
                                    return new SnapshotWatches.FilteredWchc(new WithoutSessionsWchc(ImmutableSet.of(input)));
                                }
                            });
            final Callable<Optional<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>>> call = new Callable<Optional<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>>>() {
                @Override
                public Optional<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>> call()
                        throws Exception {
                    if (!localTransformer.isDone()) {
                        return Optional.absent();
                    }
                    ImmutableList.Builder<Supplier<? extends ListenableFuture<FourLetterWords.Wchc>>> servers = ImmutableList.builder();
                    for (ServerInetAddressView e: ensemble) {
                        final InetSocketAddress address = e.get();
                        final Supplier<? extends ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>> connections = new Supplier<ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>>() {
                            @Override
                            public ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> get() {
                                return anonymous.connect(address);
                            }
                        };
                        final Function<FourLetterWords.Wchc, FourLetterWords.Wchc> transformWchc = server.get().equals(address) ? Functions.compose(transformer, localTransformer.get()) : transformer;
                        servers.add(SnapshotWatches.futureTransform(
                                SnapshotWatches.transform(
                                        connections, 
                                        SnapshotWatches.QueryWchc.create()),
                                transformWchc));
                    }
                    final Supplier<ListenableFuture<AsyncFunction<Long,Long>>> getSessions = new Supplier<ListenableFuture<AsyncFunction<Long,Long>>>() {
                        @SuppressWarnings("unchecked")
                        final FixedQuery<?> query = FixedQuery.forIterable(
                                materializer, 
                                PathToRequests.forRequests(
                                        Operations.Requests.sync(), 
                                        Operations.Requests.getChildren())
                                        .apply(StorageSchema.Safari.Sessions.PATH));
                        
                        @Override
                        public ListenableFuture<AsyncFunction<Long, Long>> get() {
                            return Futures.transform(
                                    Futures.<Operation.ProtocolResponse<?>>allAsList(query.call()),
                                    new AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, AsyncFunction<Long,Long>>() {
                                        @Override
                                        public ListenableFuture<AsyncFunction<Long, Long>> apply(
                                                List<? extends Operation.ProtocolResponse<?>> input)
                                                throws Exception {
                                            for (Operation.ProtocolResponse<?> response: input) {
                                                Operations.unlessError(response.record());
                                            }
                                            return Futures.immediateFuture(sessions);
                                        }
                            });
                        }
                    };
                    return Optional.<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>>of(SnapshotWatches.create(prefix, labelOf, materializer.codec(), client, getSessions, servers.build()));
                }
            };
            CallablePromiseTask<?, ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>> task = CallablePromiseTask.create(
                    call, 
                    SettableFuturePromise.<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>>create());
            localTransformer.addListener(task, MoreExecutors.directExecutor());
            return new PrepareSnapshotEnsembleWatches(task);
        }
        
        protected PrepareSnapshotEnsembleWatches(
                ListenableFuture<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>> delegate) {
            super(delegate);
        }
    }

    public static final class CallSnapshotEnsembleWatches extends Action<FourLetterWords.Wchc> {

        
        @SuppressWarnings("unchecked")
        public static ListenableFuture<FourLetterWords.Wchc> create(
                ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> chain) {
            Optional<FourLetterWords.Wchc> result;
            try {
                result = chain.call();
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
            if (result.isPresent()) {
                return Futures.immediateFuture(result.get());
            } else {
                return new CallSnapshotEnsembleWatches(chain, (ListenableFuture<FourLetterWords.Wchc>) chain.chain().chain().getLast());
            }
        }
        
        protected final ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> chain;
        
        public CallSnapshotEnsembleWatches(
                ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> chain,
                ListenableFuture<FourLetterWords.Wchc> future) {
            super(future);
            this.chain = chain;
        }
        
        public ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> chain() {
            return chain;
        }
    }
    
    public static final class CreateSnapshot extends Action<Long> {
        
        public static CreateSnapshot call(ListenableFuture<Long> future) {
            return new CreateSnapshot(future);
        }
        
        protected CreateSnapshot(ListenableFuture<Long> future) {
            super(future);
        }
    }

    /**
     * Sort children so that sequential nodes are processed in sequence order.
     */
    public static final class SortedChildrenIterator implements Processor<SubmittedRequest<Records.Request,?>, Iterator<AbsoluteZNodePath>> {
    
        public static SortedChildrenIterator create() {
            return new SortedChildrenIterator();
        }
        
        protected SortedChildrenIterator() {}
        
        @Override
        public Iterator<AbsoluteZNodePath> apply(SubmittedRequest<Records.Request,?> input) throws Exception {
                final Records.Response response = input.get().record();
            if (response instanceof Records.ChildrenGetter) {
                final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.getValue()).getPath());
                final ImmutableSortedSet<String> children = 
                        ImmutableSortedSet.copyOf(
                                Sequential.comparator(), 
                                ((Records.ChildrenGetter) response).getChildren());
                return Iterators.transform(
                                children.iterator(),
                                TreeWalker.ChildToPath.forParent(path));
            } else {
                return ImmutableSet.<AbsoluteZNodePath>of().iterator();
            }
        }
    }
    
    public static abstract class RunOnEmptyActor<I,V,T extends ListenableFuture<V>> extends ListenableFutureActor<I,V,T> {

        protected final Runnable runnable;
        
        protected RunOnEmptyActor(
                Runnable runnable,
                Queue<T> mailbox,
                Logger logger) {
            super(mailbox, logger);
            this.runnable = runnable;
        }
        
        public boolean isEmpty() {
            return mailbox.isEmpty();
        }

        @Override
        protected boolean apply(T input) throws Exception {
            synchronized (runnable) {
                boolean apply = super.apply(input);
                if (isEmpty()) {
                    runnable.run();
                }
                return apply;
            }
        }
    }
    
    protected final class WalkerProcessor extends ForwardingListenableFuture<Long> implements Processor<Optional<? extends SubmittedRequest<Records.Request,?>>, Optional<ListenableFuture<Long>>>, Runnable, TaskExecutor<Pair<Optional<Long>,? extends Records.Request>, O> {

        protected final Promise<Long> promise;
        protected final ToClient submitTo;
        protected final EphemeralZNodeLookups ephemerals;
        protected final FromListener changes;
        protected final TreeWalker.Builder<ListenableFuture<Long>> walker;
        protected final ZxidTracker fromZxid;
        protected int walkers;
        protected int pending;
        
        public WalkerProcessor(
                ZxidTracker fromZxid,
                Promise<Long> promise) {
            this.fromZxid = fromZxid;
            this.promise = promise;
            this.walkers = 0;
            this.pending = 0;
            this.walker = TreeWalker.forResult(this)
                    .setIterator(SortedChildrenIterator.create())
                    .setRequests(TreeWalker.toRequests(
                            TreeWalker.parameters()
                                .setData(true)
                                .setWatch(true)
                                .setSync(true)))
                    .setClient(fromClient);
            this.submitTo = Actors.stopWhenDone(new ToClient(), this);
            this.ephemerals = Actors.stopWhenDone(
                    new EphemeralZNodeLookups(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.pathOf(toVolume.getVolume(), toVolume.getVersion()),
                        new Function<ZNodePath,ZNodeLabel>() {
                            @Override
                            public ZNodeLabel apply(ZNodePath input) {
                                return (ZNodeLabel) StorageZNode.EscapedNamedZNode.converter().convert(
                                                input.suffix(toRoot));
                            }
                        }), this);
            this.changes = new FromListener();
            
            addListener(this, MoreExecutors.directExecutor());
        }

        public synchronized TreeWalker<ListenableFuture<Long>> walk(ZNodePath root) {
            ++walkers;
            return walker.setRoot(root).build();
        }
        
        @Override
        public synchronized void run() {
            if (!isDone()) {
                if ((walkers == 0) && (pending == 0) && submitTo.isEmpty() && ephemerals.isEmpty() && changes.isEmpty()) {
                    promise.set(Long.valueOf(fromZxid.get()));
                }
            }
        }

        @Override
        public ListenableFuture<O> submit(Pair<Optional<Long>,? extends Records.Request> request) {
            ListenableFuture<O> future = submitTo.submit(request.second());
            if (request.first().isPresent()) {
                updateZxid(request.first().get().longValue(), future);
            }
            return future;
        }
        
        @Override
        public synchronized Optional<ListenableFuture<Long>> apply(
                Optional<? extends SubmittedRequest<Records.Request, ?>> input)
                throws Exception {
            if (input.isPresent()) {
                final Operation.ProtocolResponse<?> response = input.get().get();
                if (Operations.maybeError(response.record(), KeeperException.Code.NONODE).isPresent()) {
                    if (((Records.PathGetter) input.get().getValue()).getPath().length() == fromRoot.length()) {
                        // root must exist
                        throw new KeeperException.NoNodeException(fromRoot.toString());
                    }
                } else {
                    if (input.get().getValue() instanceof IGetDataRequest) {
                        final long zxid = response.zxid();
                        final AbsoluteZNodePath fromPath = AbsoluteZNodePath.fromString(((Records.PathGetter) input.get().getValue()).getPath());
                        final Records.ZNodeStatGetter stat = new IStat(((Records.StatGetter) response.record()).getStat());
                        CreateMode mode = (stat.getEphemeralOwner() == Stats.CreateStat.ephemeralOwnerNone()) ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
                        final Optional<? extends Sequential<String,?>> sequential;
                        if (fromPath.length() > fromRoot.length()) {
                            sequential = Sequential.maybeFromString(fromPath.label().toString());
                        } else {
                            assert (fromPath.length() == fromRoot.length());
                            sequential = Optional.absent();
                        }
                        final ZNodePath toPath;
                        if (sequential.isPresent()) {
                            mode = mode.sequential();
                            toPath = paths.apply(((AbsoluteZNodePath) fromPath).parent().join(ZNodeLabel.fromString(sequential.get().prefix())));
                        } else {
                            toPath = paths.apply(fromPath);
                        }
                        final Operations.Requests.Create create = Operations.Requests.create()
                                .setMode(mode)
                                .setPath(toPath)
                                .setData(((Records.DataGetter) response.record()).getData());
                        if (mode.contains(CreateFlag.EPHEMERAL)) {
                            ephemerals.submit(
                                    StampedValue.valueOf(
                                            zxid, ZNode.create(create, stat)));
                        } else {
                            Records.Request request;
                            if (toPath.length() > toRoot.length()) {
                                request = create.build();
                            } else {
                                assert (toPath.length() == toRoot.length());
                                request = Operations.Requests.setData()
                                        .setPath(create.getPath())
                                        .setData(create.getData()).build();
                            }
                            submit(Pair.create(
                                    Optional.of(Long.valueOf(zxid)), 
                                    request));
                        }
                    }    
                }
                return Optional.absent();
            } else {
                assert (walkers > 0);
                if (--walkers == 0) {
                    run();
                }
                return Optional.<ListenableFuture<Long>>of(this);
            }
        }
        
        protected synchronized <V,T extends ListenableFuture<V>> T updateZxid(long zxid, T future) {
            new ZxidUpdater<V>(zxid, future);
            return future;
        }
        
        @Override
        protected ListenableFuture<Long> delegate() {
            return promise;
        }
        
        protected final class ZxidUpdater<V> extends ForwardingListenableFuture<V> implements Runnable {
            
            private final long zxid;
            private final ListenableFuture<V> future;
            
            public ZxidUpdater(
                    long zxid,
                    ListenableFuture<V> future) {
                this.zxid = zxid;
                this.future = future;
                ++pending;
                addListener(this, MoreExecutors.directExecutor());
            }

            @Override
            public void run() {
                if (isDone()) {
                    try {
                        get();
                        WalkerProcessor.this.fromZxid.update(this.zxid);
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    } catch (ExecutionException e) {
                    } finally {
                        synchronized (WalkerProcessor.this) {
                            if (--pending == 0) {
                                WalkerProcessor.this.run();
                            }
                        }
                    }
                }
            }

            @Override
            protected ListenableFuture<V> delegate() {
                return future;
            }
        }
        
        protected final class ToClient extends SubmitActor<Records.Request, O> {

            public ToClient() {
                super(toClient,
                        Queues.<SubmittedRequest<Records.Request,O>>newConcurrentLinkedQueue(),
                        LogManager.getLogger(ToClient.class));
            }
            
            public boolean isEmpty() {
                return mailbox.isEmpty();
            }
            
            @Override
            public void run() {
                super.run();
            }
            
            @Override
            protected boolean doApply(SubmittedRequest<Records.Request, O> input) throws Exception {
                synchronized (WalkerProcessor.this) {
                    O response = input.get();
                    if (input.getValue() instanceof ICreateRequest) {
                        ICreateRequest request = (ICreateRequest) input.getValue();
                        Optional<Operation.Error> error = Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS);
                        if (CreateMode.valueOf(request.getFlags()) == CreateMode.PERSISTENT_SEQUENTIAL) {
                            if (!error.isPresent()) {
                                paths.add(ZNodePath.fromString(request.getPath()), ZNodePath.fromString(((ICreateResponse) response.record()).getPath()));
                            } else {
                                // TODO
                                throw new UnsupportedOperationException();
                            }
                        }
                    } else if (input.getValue() instanceof IDeleteRequest) {
                        Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                    } else {
                        // TODO
                    }
                    if (isEmpty()) {
                        WalkerProcessor.this.run();
                    }
                }
                return true;
            }
        }
        
        protected final class EphemeralZNodeLookups extends RunOnEmptyActor<StampedValue<ZNode>, Long, ValueFuture<StampedValue<ZNode>,Long,?>> {

            protected final ZNodePath prefix;
            protected final Function<ZNodePath,ZNodeLabel> labelOf;
            
            public EphemeralZNodeLookups(
                    ZNodePath prefix,
                    Function<ZNodePath,ZNodeLabel> labelOf) {
                super(WalkerProcessor.this,
                        Queues.<ValueFuture<StampedValue<ZNode>,Long,?>>newConcurrentLinkedQueue(),
                        LogManager.getLogger(EphemeralZNodeLookups.class));
                this.prefix = prefix;
                this.labelOf = labelOf;
            }

            @Override
            public ListenableFuture<Long> submit(StampedValue<ZNode> value) {
                ListenableFuture<Long> future;
                try {
                    future = sessions.apply(
                            Long.valueOf(
                                    value.get().getStat().getEphemeralOwner()));
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                ValueFuture<StampedValue<ZNode>, Long, ?> lookup =  ValueFuture.create(
                        value, future);
                if (!send(lookup)) {
                    lookup.cancel(false);
                }
                return lookup;
            }
            
            @Override
            protected boolean doApply(ValueFuture<StampedValue<ZNode>, Long, ?> input) throws Exception {
                Long session = input.get();
                if (session == null) {
                    return true;
                }
                ZNodePath path = prefix.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Session.labelOf(session.longValue()));
                WalkerProcessor.this.submit(Pair.create(Optional.<Long>absent(), Operations.Requests.create().setPath(path).build()));
                path = path.join(
                        labelOf.apply(input.getValue().get().getCreate().getPath()));
                ByteBufOutputArchive buf = new ByteBufOutputArchive(Unpooled.buffer());
                Records.Requests.serialize(input.getValue().get().getCreate().build(), buf);
                WalkerProcessor.this.submit(Pair.create(
                        Optional.of(Long.valueOf(input.getValue().stamp())), 
                        Operations.Requests.create().setPath(path).setData(buf.get().array()).build()));
                return true;
            }
        }
        
        protected final class FromListener implements SessionListener, Runnable {
        
            protected final Set<Change<?>> changes;
            
            public FromListener() {
                this.changes = Sets.newHashSet();
                fromClient.subscribe(this);
                WalkerProcessor.this.addListener(this, MoreExecutors.directExecutor());
            }
            
            public boolean isEmpty() {
                synchronized (WalkerProcessor.this) {
                    return changes.isEmpty();
                }
            }
            
            @Override
            public void handleAutomatonTransition(
                    Automaton.Transition<ProtocolState> transition) {
                // TODO Auto-generated method stub
            }
        
            @Override
            public void handleNotification(
                    Operation.ProtocolResponse<IWatcherEvent> notification) {
                synchronized (WalkerProcessor.this) {
                    Optional<? extends Change<?>> change = Optional.absent();
                    switch (Watcher.Event.EventType.fromInt(notification.record().getType())) {
                    case NodeChildrenChanged:
                    {
                        ZNodePath path = ZNodePath.fromString(notification.record().getPath());
                        change = Optional.of(new ChildrenCreated(path));
                        break;
                    }
                    case NodeDataChanged:
                    {
                        ZNodePath path = ZNodePath.fromString(notification.record().getPath());
                        change = Optional.of(new DataChanged(path));
                        break;
                    }
                    case NodeDeleted:
                    {
                        ZNodePath path = ZNodePath.fromString(notification.record().getPath());
                        if (path.length() == fromRoot.length()) {
                            // root must exist
                            WalkerProcessor.this.promise.setException(
                                    new KeeperException.NoNodeException(path.toString()));
                        }
                        WalkerProcessor.this.submit(
                                Pair.create(
                                        Optional.<Long>absent(),
                                        Operations.Requests.delete().setPath(
                                                paths.apply(path)).build()));
                        break;
                    }
                    default:
                        break;
                    }
                    if (change != null) {
                        changes.add(change.get());
                        change.get().addListener(change.get(), MoreExecutors.directExecutor());
                    }
                }
            }
        
            @Override
            public void run() {
                synchronized (WalkerProcessor.this) {
                    if (WalkerProcessor.this.isDone()) {
                        fromClient.unsubscribe(this);
                        for (Change<?> change: Iterables.consumingIterable(changes)) {
                            change.cancel(false);
                        }
                    }
                }
            }

            
            protected abstract class Change<V> extends ForwardingListenableFuture<V> implements Runnable {

                protected final ZNodePath fromPath;
                protected final ZNodePath toPath;
                
                protected Change(
                        ZNodePath fromPath) {
                    this.fromPath = fromPath;
                    this.toPath = paths.apply(fromPath);
                }
                
                @Override
                public void run() {
                    synchronized (WalkerProcessor.this) {
                        if (isDone()) {
                            changes.remove(this);
                            try {
                                doRun();
                            } catch (Exception e) {
                                promise.setException(e);
                            }
                        }
                    }
                }
                
                protected abstract void doRun() throws Exception;
            }

            protected final class ChildrenCreated extends Change<List<List<O>>> {

                protected final ListenableFuture<List<List<O>>> future;
                protected final SubmittedRequests<? extends Records.Request,O> getFromChildren;
                protected final SubmittedRequests<? extends Records.Request,O> getToChildren;
                
                @SuppressWarnings("unchecked")
                public ChildrenCreated(
                        ZNodePath fromPath) {
                    super(fromPath);
                    ImmutableList<? extends Operations.PathBuilder<? extends Records.Request, ?>> builders = ImmutableList.of(
                            Operations.Requests.sync(), Operations.Requests.getChildren().setWatch(true));
                    ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
                    for (Operations.PathBuilder<? extends Records.Request, ?> builder: builders) {
                        requests.add(builder.setPath(fromPath).build());
                    }
                    this.getFromChildren = SubmittedRequests.submit(
                            fromClient, requests.build());
                    requests = ImmutableList.builder();
                    for (Operations.PathBuilder<? extends Records.Request, ?> builder: builders) {
                        requests.add(builder.setPath(toPath).build());
                    }
                    this.getToChildren = SubmittedRequests.submit(
                            toClient, requests.build());
                    this.future = Futures.allAsList(getFromChildren, getToChildren);
                }

                @Override
                protected void doRun() throws Exception {
                    final List<O> fromResponses = getFromChildren.get();
                    final List<O> toResponses = getToChildren.get();
                    for (int i=0; i<fromResponses.size(); ++i) {
                        final Records.Response fromResponse = fromResponses.get(i).record();
                        final Optional<Operation.Error> fromError = Operations.maybeError(fromResponse, KeeperException.Code.NONODE);
                        final Records.Response toResponse = toResponses.get(i).record();
                        final Optional<Operation.Error> toError = Operations.maybeError(fromResponse, KeeperException.Code.NONODE);
                        if (getFromChildren.getValue().get(i).opcode() == OpCode.GET_CHILDREN) {
                            if (fromError.isPresent()) {
                                if (!toError.isPresent()) {
                                    WalkerProcessor.this.submit(
                                            Pair.create(
                                                    Optional.of(Long.valueOf(fromResponses.get(i).zxid())),
                                                    Operations.Requests.delete().setPath(toPath).build()));
                                }
                            } else {
                                if (!toError.isPresent()) {
                                    List<String> created = ImmutableList.of();
                                    final Set<String> fromChildren = ImmutableSet.copyOf(((Records.ChildrenGetter) fromResponse).getChildren());
                                    final Set<String> toChildren = ImmutableSet.copyOf(((Records.ChildrenGetter) toResponse).getChildren());
                                    for (String fromChild: fromChildren) {
                                        String toChild = null;
                                        Optional<? extends Sequential<String,?>> sequential = Sequential.maybeFromString(fromChild);
                                        if (sequential.isPresent()) {
                                            try {
                                                toChild = ((AbsoluteZNodePath) paths.apply(fromPath.join(ZNodeLabel.fromString(fromChild)))).label().toString();
                                            } catch (IllegalArgumentException e) {
                                            }
                                        } else {
                                            toChild = fromChild;
                                        }
                                        if ((toChild == null) || !toChildren.contains(toChild)) {
                                            if (created.isEmpty()) {
                                                created = Lists.newLinkedList();
                                            }
                                            created.add(fromChild);
                                        }
                                    }
                                    for (String child: created) {
                                        updateZxid(
                                                fromResponses.get(i).zxid(),
                                                WalkerProcessor.this.walk(fromPath.join(ZNodeLabel.fromString(child))));
                                    }
                                } else {
                                    // TODO
                                    throw new KeeperException.NoNodeException(toPath.toString());
                                }
                            }
                        }
                    }
                }

                @Override
                protected ListenableFuture<List<List<O>>> delegate() {
                    return future;
                }
            }
            
            protected final class DataChanged extends Change<List<O>> {
                
                protected final SubmittedRequests<? extends Records.Request,O> getData;
                
                public DataChanged(
                        ZNodePath fromPath) {
                    super(fromPath);
                    ImmutableList<? extends Operations.PathBuilder<? extends Records.Request, ?>> builders = ImmutableList.of(
                            Operations.Requests.sync(), Operations.Requests.getData().setWatch(true));
                    ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
                    for (Operations.PathBuilder<? extends Records.Request, ?> builder: builders) {
                        requests.add(builder.setPath(fromPath).build());
                    }
                    this.getData = SubmittedRequests.submit(
                            fromClient, requests.build());
                }
                
                @Override
                protected void doRun() throws Exception {
                    Pair<Optional<Long>,? extends Records.Request> request = null;
                    List<O> responses = getData.get();
                    for (int i=0; i<responses.size(); ++i) {
                        final Records.Response response = responses.get(i).record();
                        final Optional<Operation.Error> error = Operations.maybeError(response, KeeperException.Code.NONODE);
                        if (getData.getValue().get(i).opcode() == OpCode.GET_DATA) {
                            Operations.PathBuilder<? extends Records.Request, ?> record;
                            if (!error.isPresent()) {
                                record = Operations.Requests.setData().setData(((Records.DataGetter) response).getData());
                            } else {
                                record = Operations.Requests.delete();
                            }
                            request = Pair.create(
                                    Optional.of(Long.valueOf(responses.get(i).zxid())),
                                    record.setPath(toPath).build());
                        }
                    }
                    assert (request != null);
                    // TODO check for error?
                    WalkerProcessor.this.submit(request);
                }

                @Override
                protected ListenableFuture<List<O>> delegate() {
                    return getData;
                }
            }
        }
    }
    
    public static final class ZNode extends AbstractPair<Operations.Requests.Create, Records.ZNodeStatGetter> {

        public static ZNode create(Operations.Requests.Create create, Records.ZNodeStatGetter stat) {
            return new ZNode(create, stat);
        }
        
        protected ZNode(Operations.Requests.Create create, Records.ZNodeStatGetter stat) {
            super(create, stat);
        }
        
        public Operations.Requests.Create getCreate() {
            return first;
        }
        
        public Records.ZNodeStatGetter getStat() {
            return second;
        }
    }
    
    protected static final class PathTranslator extends PrefixTranslator {

        public static PathTranslator empty(
                ZNodePath fromPrefix, 
                ZNodePath toPrefix) {
            return new PathTranslator(
                    SimpleNameTrie.forRoot(ValueNode.<ZNodeName>root(EmptyZNodeLabel.getInstance())),
                    fromPrefix,
                    toPrefix);
        }
        
        private final SimpleNameTrie<ValueNode<ZNodeName>> trie;
        
        protected PathTranslator(
                SimpleNameTrie<ValueNode<ZNodeName>> trie,
                ZNodePath fromPrefix, 
                ZNodePath toPrefix) {
            super(fromPrefix, toPrefix);
            this.trie = trie;
        }
        
        public SimpleNameTrie<ValueNode<ZNodeName>> trie() {
            return trie;
        }
        
        public boolean add(ZNodePath from, ZNodePath to) {
            checkArgument(Sequential.maybeFromString(((AbsoluteZNodePath) from).label()).isPresent());
            checkArgument(Sequential.maybeFromString(((AbsoluteZNodePath) to).label()).isPresent());
            ValueNode<ZNodeName> node = trie.root();
            Iterator<ZNodeName> fromPrefixes = SequenceIterator.forName(from);
            Iterator<ZNodeName> toPrefixes = SequenceIterator.forName(to);
            boolean updated = false;
            while (fromPrefixes.hasNext()) {
                ZNodeName fromPrefix = fromPrefixes.next();
                ZNodeName toPrefix = toPrefixes.next();
                ValueNode<ZNodeName> next = node.get(fromPrefix);
                if (next == null) {
                    next = ValueNode.child(toPrefix, fromPrefix, node);
                    node.put(fromPrefix, next);
                    if (!fromPrefixes.hasNext()) {
                        updated = true;
                    }
                } else {
                    checkArgument(fromPrefix.equals(toPrefix));
                }
            }
            return updated;
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(super.toString()).addValue(trie).toString();
        }

        @Override
        protected ZNodePath join(ZNodeName remaining) {
            ValueNode<ZNodeName> node = trie.root();
            Iterator<ZNodeName> prefixes = SequenceIterator.forName(remaining);
            List<String> names = Lists.newLinkedList();
            while (prefixes.hasNext()) {
                final ZNodeName next = prefixes.next();
                node = node.get(next);
                final String name;
                if (node != null) {
                    name = node.get().toString();
                } else {
                    checkArgument(!prefixes.hasNext(), String.valueOf(next));
                    name = next.toString();
                }
                names.add(name);
            }
            return super.join(ZNodeName.fromString(ZNodeLabelVector.join(names.iterator())));
        }
        
        public static final class SequenceIterator extends AbstractIterator<ZNodeName> {

            public static SequenceIterator forName(ZNodeName name) {
                final Iterator<ZNodeLabel> labels;
                if (name instanceof EmptyZNodeLabel) {
                    labels = ImmutableSet.<ZNodeLabel>of().iterator();
                } else if (name instanceof ZNodeLabel) {
                    labels = Iterators.singletonIterator((ZNodeLabel) name);
                } else {
                    labels = ((RelativeZNodePath) name).iterator();
                }
                return forLabels(labels);
            }
            
            public static SequenceIterator forLabels(Iterator<ZNodeLabel> labels) {
                return new SequenceIterator(labels);
            }
            
            private final Iterator<ZNodeLabel> labels;

            private SequenceIterator(Iterator<ZNodeLabel> labels) {
                this.labels = labels;
            }

            @Override
            protected ZNodeName computeNext() {
                if (!labels.hasNext()) {
                    return endOfData();
                } else {
                    List<String> next = Lists.newLinkedList();
                    Optional<? extends Sequential<String,?>> sequential;
                    do {
                        String label = labels.next().toString();
                        next.add(label);
                        sequential = Sequential.maybeFromString(label);
                    } while (labels.hasNext() && !sequential.isPresent());
                    return ZNodeName.fromString((next.size() == 1) ? Iterables.getOnlyElement(next) : ZNodeLabelVector.join(next.iterator()));
                }
            }
        }
    }
}
