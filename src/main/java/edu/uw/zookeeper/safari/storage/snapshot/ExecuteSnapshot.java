package edu.uw.zookeeper.safari.storage.snapshot;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.ImmutableClassToInstanceMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
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
import edu.uw.zookeeper.client.ChildToPath;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.TreeWalker;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.ForwardingPromise.SimpleForwardingPromise;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.ListenableFutureActor;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.SimpleNameTrie;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchEvent;
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
import edu.uw.zookeeper.protocol.proto.IGetDataRequest;
import edu.uw.zookeeper.protocol.proto.IGetDataResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Stats;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.EscapedConverter;
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

    /**
     * Assumes volume log version exists at the destination.
     */
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
                                    ExecuteSnapshot.Action<?> action = actions.next();
                                    Object result = action.get();
                                    if (action instanceof CreateSnapshot) {
                                        return (Long) ((Pair<?,?>) result).first();
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
    protected final SequentialTranslator<O> sequentials;
    
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
        this.sessions = sessions;
        this.sequentials = SequentialTranslator.create(fromRoot, toRoot, toClient);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Optional<? extends Action<?>> apply(ChainedFutures.DequeChain<Action<?>,?> input) throws Exception {
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
            return Optional.of(CreateSnapshotSkeleton.create(toVolume.getVolume(), toVolume.getVersion(), toVolume.getBranch(), materializer.codec(), toClient));
        } else if (last.get() instanceof CreateSnapshotSkeleton) {
            return Optional.of(
                    PrepareSnapshotWatches.create(
                            StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.pathOf(toVolume.getVolume(), toVolume.getVersion()),
                        new Function<ZNodePath,ZNodeLabel>() {
                            @Override
                            public ZNodeLabel apply(ZNodePath input) {
                                // all sequential persistent znodes should have been created by now
                                try {
                                    return ZNodeLabel.fromString(EscapedConverter.getInstance().convert(
                                                    sequentials.apply(input).get().suffix(toRoot).toString()));
                                } catch (InterruptedException e) {
                                    throw Throwables.propagate(e);
                                } catch (Exception e) {
                                    throw new IllegalStateException(e);
                                }
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
        } else if (last.get() instanceof PrepareSnapshotWatches) {
            try {
                return Optional.of((CallSnapshotWatches)
                        CallSnapshotWatches.create(
                                ((PrepareSnapshotWatches) last.get()).get()));
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
        } else if (last.get() instanceof CallSnapshotWatches) {
            Action<?> previous = Iterators.get(input.descendingIterator(), 1);
            if (previous instanceof PrepareSnapshotWatches) {
                return Optional.of(
                        CreateSnapshot.call(
                                new WalkSnapshotter(ZxidTracker.zero())));
            } else if (previous instanceof CreateSnapshot) {
                return Optional.absent();
            } else {
                throw new AssertionError();
            }
        } else if (last.get() instanceof CreateSnapshot) {
            CallSnapshotWatches watches = (CallSnapshotWatches) Iterators.get(input.descendingIterator(), 1);
            ((ChainedFutures<ListenableFuture<?>,?,?>) watches.chain().chain()).add(last.get());
            return Optional.of((CallSnapshotWatches) CallSnapshotWatches.create(watches.chain()));
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
    public static final class CreateSnapshotSkeleton extends Action<Boolean> {
        
        public static <O extends Operation.ProtocolResponse<?>> CreateSnapshotSkeleton create(
                Identifier toVolume,
                UnsignedLong toVersion,
                ZNodeName toBranch,
                Serializers.ByteSerializer<Object> serializer,
                ClientExecutor<? super Records.Request,O,?> client) {
            final AbsoluteZNodePath snapshotPath = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.pathOf(toVolume, toVersion);
            Operations.Requests.Create create = Operations.Requests.create();
            ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
            AbsoluteZNodePath path = snapshotPath;
            requests.add(create.setPath(path).build());
            path = path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Prefix.LABEL);
            try {
                requests.add(create.setPath(path).setData(serializer.toBytes(toBranch)).build());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            path = snapshotPath.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL);
            requests.add(create.setPath(path).build());
            path = path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.LABEL);
            requests.add(create.setPath(path).build());
            path = snapshotPath.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.LABEL);
            requests.add(create.setPath(path).build());
            path = path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.LABEL);
            requests.add(create.setPath(path).build());
            return new CreateSnapshotSkeleton(Created.create(requests.build(), client));
        }
        
        protected CreateSnapshotSkeleton(ListenableFuture<Boolean> future) {
            super(future);
        }

        protected static class Created<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> implements Callable<Optional<Boolean>> {

            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Boolean> create(
                    List<? extends Records.Request> requests,
                    ClientExecutor<? super Records.Request,O,?> client) {
                CallablePromiseTask<Created<O>,Boolean> task = CallablePromiseTask.create(new Created<O>(
                        SubmittedRequests.submit(client, requests)), 
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
    
    public static final class PrepareSnapshotWatches extends Action<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>> {
        
        public static <O extends Operation.ProtocolResponse<?>> PrepareSnapshotWatches create(
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
            return new PrepareSnapshotWatches(task);
        }
        
        protected PrepareSnapshotWatches(
                ListenableFuture<ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?>> delegate) {
            super(delegate);
        }
    }

    public static final class CallSnapshotWatches extends Action<FourLetterWords.Wchc> {

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
                return new CallSnapshotWatches(chain, (ListenableFuture<FourLetterWords.Wchc>) chain.chain().chain().getLast());
            }
        }
        
        protected final ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> chain;
        
        public CallSnapshotWatches(
                ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> chain,
                ListenableFuture<FourLetterWords.Wchc> future) {
            super(future);
            this.chain = chain;
        }
        
        public ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> chain() {
            return chain;
        }
    }
    
    public static final class CreateSnapshot extends Action<Pair<Long,Map<ZNodePath,Long>>> {
        
        public static CreateSnapshot call(Callable<? extends ListenableFuture<Pair<Long,Map<ZNodePath,Long>>>> callable) {
            ListenableFuture<Pair<Long,Map<ZNodePath,Long>>> future;
            try {
                future = callable.call();
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return new CreateSnapshot(future);
        }
        
        protected CreateSnapshot(ListenableFuture<Pair<Long,Map<ZNodePath,Long>>> future) {
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
        
        private static final Comparator<String> COMPARATOR = Sequential.comparator();

        protected SortedChildrenIterator() {}
        
        @Override
        public Iterator<AbsoluteZNodePath> apply(SubmittedRequest<Records.Request,?> input) throws Exception {
                final Records.Response response = input.get().record();
            if (response instanceof Records.ChildrenGetter) {
                final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.getValue()).getPath());
                final ImmutableSortedSet<String> children = 
                        ImmutableSortedSet.copyOf(
                                COMPARATOR, 
                                ((Records.ChildrenGetter) response).getChildren());
                return Iterators.transform(
                                children.iterator(),
                                ChildToPath.forParent(path));
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
    
    protected final class WalkSnapshotter extends ForwardingListenableFuture<Pair<Long,Map<ZNodePath, Long>>> implements Callable<ListenableFuture<Pair<Long,Map<ZNodePath, Long>>>>, AsyncFunction<ZNodePath, Long>, FutureCallback<Long>, Runnable, TaskExecutor<StampedValue<? extends Operations.PathBuilder<? extends Records.Request,?>>, Pair<ZNodePath,O>> {

        protected final Logger logger;
        protected final Promise<Pair<Long,Map<ZNodePath, Long>>> promise;
        protected final Map<Walker, ListenableFuture<Long>> walkers;
        protected final EphemeralsSnapshotter<O> ephemerals;
        protected final NotificationsSnapshotter<O> changes;
        protected final TreeWalker.Builder<ListenableFuture<Long>> walk;
        protected final ZxidTracker fromZxid;
        protected final Map<ZNodePath, Long> ephemeralOwners;
        
        protected WalkSnapshotter(
                ZxidTracker fromZxid) {
            this.logger = LogManager.getLogger(this);
            this.fromZxid = fromZxid;
            this.promise = SettableFuturePromise.create();
            this.ephemeralOwners = Maps.newHashMap();
            this.walk = TreeWalker.<ListenableFuture<Long>>builder()
                    .setIterator(SortedChildrenIterator.create())
                    .setRequests(TreeWalker.toRequests(
                            TreeWalker.parameters()
                                .setData(true)
                                .setWatch(true)
                                .setSync(true)))
                    .setClient(fromClient);
            this.ephemerals = EphemeralsSnapshotter.create(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.pathOf(toVolume.getVolume(), toVolume.getVersion()),
                    toRoot,
                    sessions,
                    sequentials,
                    toClient);
            this.walkers = Maps.newHashMap();
            this.changes = NotificationsSnapshotter.create(
                    this, this, this, fromClient, logger);

            addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public ListenableFuture<Pair<Long,Map<ZNodePath, Long>>> call() throws Exception {
            if (!isDone()) {
                apply(fromRoot);
                fromClient.subscribe(changes);
            }
            return this;
        }

        @Override
        public synchronized ListenableFuture<Pair<ZNodePath,O>> submit(StampedValue<? extends Operations.PathBuilder<? extends Records.Request,?>> request) {
            final ListenableFuture<Pair<ZNodePath,O>> future;
            if (request.get() instanceof Operations.Requests.Create) {
                if (request.stamp() != Stats.CreateStat.ephemeralOwnerNone()) {
                    ephemeralOwners.put(request.get().getPath(), Long.valueOf(request.stamp()));
                    future = ephemerals.submit(request);
                } else {
                    future = sequentials.submit(request.get());
                }
            } else {
                Long owner;
                if (request.get() instanceof Operations.Requests.Delete) {
                    owner = ephemeralOwners.remove(request.get().getPath());
                } else {
                    owner = ephemeralOwners.get(request.get().getPath());
                }
                if (owner != null) {
                    future = ephemerals.submit(StampedValue.valueOf(owner.longValue(), request.get()));
                } else {
                    future = sequentials.submit(request.get());
                }
            }
            return LoggingFutureListener.listen(logger, future);
        }

        @Override
        public synchronized void onSuccess(Long result) {
            if (!isDone()) {
                fromZxid.update(result);
                run();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            promise.setException(t);
        }
        
        public synchronized ListenableFuture<Long> apply(ZNodePath root) {
            final Walker walker = new Walker(SettableFuturePromise.<Long>create());
            final ListenableFuture<Long> future = Futures.transform(
                    this.walk.setRoot(root).setResult(walker).build(),
                    new GetOptional<Long>());
            walkers.put(walker, future);
            walker.run();
            run();
            return LoggingFutureListener.listen(logger, future);
        }
        
        @Override
        public synchronized void run() {
            if (!isDone()) {
                if (walkers.isEmpty() && changes.isEmpty()) {
                    // need to map backend to frontend sessions
                    ImmutableMap.Builder<ZNodePath, Long> ephemerals = ImmutableMap.builder();
                    for (Map.Entry<ZNodePath, Long> entry: ephemeralOwners.entrySet()) {
                        EphemeralsSnapshotter<?>.SessionEphemeralSnapshotter session = this.ephemerals.ephemerals.get(entry.getValue());
                        if ((session != null) && (session.prefix.isDone())) {
                            try {
                                ephemerals.put(entry.getKey(), session.prefix.get().first());
                            } catch (Exception e) {}
                        } // else ??
                    }
                    promise.set(Pair.create(Long.valueOf(fromZxid.get()), (Map<ZNodePath,Long>) ephemerals.build()));
                }
            } else {
                changes.stop();
                ephemerals.stop();
                for (Map.Entry<Walker, ? extends ListenableFuture<?>> entry: Iterables.consumingIterable(walkers.entrySet())) {
                    entry.getValue().cancel(false);
                }
            }
        }
        
        @Override
        protected ListenableFuture<Pair<Long,Map<ZNodePath, Long>>> delegate() {
            return promise;
        }
        
        protected final class Walker extends SimpleForwardingPromise<Long> implements Runnable, FutureCallback<Object>, Processor<Optional<? extends SubmittedRequest<Records.Request,?>>, Optional<ListenableFuture<Long>>> {

            protected AtomicInteger pending;
            
            protected Walker(
                    Promise<Long> delegate) {
                super(delegate);
                this.pending = new AtomicInteger(1);
                addListener(this, MoreExecutors.directExecutor());
            }
            
            @Override
            public Optional<ListenableFuture<Long>> apply(
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
                            final ZNodePath fromPath = ZNodePath.fromString(((Records.PathGetter) input.get().getValue()).getPath());
                            final IGetDataResponse getData = (IGetDataResponse) response.record();
                            ListenableFuture<?> future;
                            if (fromPath.length() == fromRoot.length()) {
                                future = UnlessError.create(
                                        toClient.submit(
                                            Operations.Requests.setData()
                                            .setPath(toRoot)
                                            .setData(getData.getData()).build()));
                            } else {
                                assert (fromPath.length() > fromRoot.length());
                                final Operations.Requests.Create create = Operations.Requests.create()
                                        .setMode(CreateMode.PERSISTENT)
                                        .setPath(fromPath)
                                        .setData(getData.getData());
                                future = submit(StampedValue.valueOf(getData.getStat().getEphemeralOwner(), create));
                            }
                            this.pending.incrementAndGet();
                            new Callback(zxid, future);
                        }    
                    }
                    return Optional.absent();
                } else {
                    onSuccess(this);
                    return Optional.<ListenableFuture<Long>>of(this);
                }
            }

            @Override
            public void onSuccess(Object result) {
                try {
                    if (result instanceof ExecuteSnapshot.WalkSnapshotter.Walker.Callback) {
                        @SuppressWarnings("unchecked")
                        Callback callback = (Callback) result;
                        try {
                            callback.future().get();
                            fromZxid.update(callback.zxid());
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }
                } finally {
                    int pending = this.pending.decrementAndGet();
                    assert (pending >= 0);
                }
                run();
            }

            @Override
            public void onFailure(Throwable t) {
                promise.setException(t);
            }
            
            @Override
            public void run() {
                if (isDone()) {
                    synchronized (WalkSnapshotter.this) {
                        if (walkers.remove(this) != null) {
                            try {
                                Long zxid = get();
                                WalkSnapshotter.this.onSuccess(zxid);
                            } catch (Exception e) {
                                WalkSnapshotter.this.onFailure(e);
                            }
                        }
                    }
                } else {
                    if (pending.get() == 0) {
                        set(Long.valueOf(fromZxid.get()));
                    }
                }
            }
            
            protected final class Callback implements Runnable {
                
                private final long zxid;
                private final ListenableFuture<?> future;
                
                public Callback(
                        long zxid,
                        ListenableFuture<?> future) {
                    this.zxid = zxid;
                    this.future = future;
                    future.addListener(this, MoreExecutors.directExecutor());
                }
                
                public long zxid() {
                    return zxid;
                }
                
                public ListenableFuture<?> future() {
                    return future;
                }

                @Override
                public void run() {
                    if (future.isDone()) {
                        onSuccess(this);
                    }
                }
            }
        }
    }
    
    /**
     * Processes one notification at a time, in order received.
     */
    protected static final class NotificationsSnapshotter<O extends Operation.ProtocolResponse<?>> extends Actors.PeekingQueuedActor<NotificationsSnapshotter<O>.Change> implements SessionListener, TaskExecutor<WatchEvent, Long> {

        public static <O extends Operation.ProtocolResponse<?>> NotificationsSnapshotter<O> create(
                final FutureCallback<Long> callback,
                final AsyncFunction<ZNodePath,Long> walker,
                final TaskExecutor<StampedValue<? extends Operations.PathBuilder<? extends Records.Request,?>>, Pair<ZNodePath,O>> toClient,
                final ClientExecutor<? super Records.Request, O, SessionListener> fromClient,
                final Logger logger) {
            return new NotificationsSnapshotter<O>(
                    callback, walker, toClient, fromClient, logger);
        }
        
        private final FutureCallback<Long> callback;
        private final ClientExecutor<? super Records.Request, O, SessionListener> fromClient;
        private final TaskExecutor<StampedValue<? extends Operations.PathBuilder<? extends Records.Request,?>>, Pair<ZNodePath,O>> toClient;
        private final AsyncFunction<ZNodePath,Long> walker;
        private final AsyncFunction<ZNodePath,List<O>> getFromData;
        private final AsyncFunction<ZNodePath,List<O>> getFromChildren;
        
        @SuppressWarnings("unchecked")
        protected NotificationsSnapshotter(
                final FutureCallback<Long> callback,
                final AsyncFunction<ZNodePath,Long> walker,
                final TaskExecutor<StampedValue<? extends Operations.PathBuilder<? extends Records.Request,?>>, Pair<ZNodePath,O>> toClient,
                final ClientExecutor<? super Records.Request, O, SessionListener> fromClient,
                final Logger logger) {
            super(Queues.<Change>newConcurrentLinkedQueue(), logger);
            this.callback = callback;
            this.walker = walker;
            this.toClient = toClient;
            this.fromClient = fromClient;
            this.getFromData = new AsyncFunction<ZNodePath,List<O>>() {
                final PathToQuery<?,O> query = PathToQuery.forRequests(
                    fromClient, 
                    Operations.Requests.sync(),
                    Operations.Requests.getData().setWatch(true));
                @Override
                public ListenableFuture<List<O>> apply(ZNodePath input) {
                    return Futures.allAsList(query.apply(input).call());
                }
            };
            this.getFromChildren = new AsyncFunction<ZNodePath,List<O>>() {
                final PathToQuery<?,O> query = PathToQuery.forRequests(
                        fromClient, 
                        Operations.Requests.sync(),
                        Operations.Requests.getChildren().setWatch(true));
                    @Override
                    public ListenableFuture<List<O>> apply(ZNodePath input) {
                        return Futures.allAsList(query.apply(input).call());
                    }
                };
        }
        
        public boolean isEmpty() {
            return mailbox.isEmpty();
        }

        @Override
        public ListenableFuture<Long> submit(WatchEvent request) {
            Change task;
            switch (request.getEventType()) {
            case NodeChildrenChanged:
            {
                task = new NodeChildrenChanged(request.getPath());
                break;
            }
            case NodeDataChanged:
            {
                task = new NodeDataChanged(request.getPath());
                break;
            }
            case NodeCreated:
            {
                task = new NodeCreated(request.getPath());
                break;
            }
            case NodeDeleted:
            {
                task = new NodeDeleted(request.getPath());
                break;
            }
            default:
                return null;
            }
            if (!send(task)) {
                throw new RejectedExecutionException();
            }
            return task;
        }
        
        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            // TODO
        }
    
        @Override
        public void handleNotification(
                Operation.ProtocolResponse<IWatcherEvent> notification) {
            try {
                submit(WatchEvent.fromRecord(notification.record()));
            } catch (RejectedExecutionException e) {
            }
        }

        @Override
        protected boolean doSend(Change message) {
            boolean sent = super.doSend(message);
            if (sent) {
                message.addListener(this, MoreExecutors.directExecutor());
            }
            return sent;
        }
        
        @Override
        protected boolean apply(Change input) throws Exception {
            if (input.isDone()) {
                if (mailbox.remove(input)) {
                    Long zxid = input.get();
                    if (mailbox.isEmpty()) {
                        callback.onSuccess(zxid);
                    }
                    return true;
                }
            } else {
                input.run();
            }
            return false;
        }
        
        @Override
        protected void doStop() {
            fromClient.unsubscribe(this);
            ListenableFuture<?> next;
            while ((next = mailbox.poll()) != null) {
                next.cancel(false);
            }
        }
        
        protected abstract class Change extends ToStringListenableFuture<Long> implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>>, Runnable {

            protected final ZNodePath fromPath;
            protected final ChainedFutures.ChainedFuturesTask<Long> delegate;
            
            protected Change(
                    ZNodePath fromPath,
                    ListenableFuture<?>...chain) {
                this(fromPath, Lists.<ListenableFuture<?>>newArrayList
                        (chain));
            }
            
            protected Change(
                    ZNodePath fromPath,
                    List<ListenableFuture<?>> chain) {
                this.fromPath = fromPath;
                this.delegate = 
                        ChainedFutures.task(
                            ChainedFutures.<Long>castLast(
                                    ChainedFutures.apply(
                                            this,
                                            ChainedFutures.list(chain))));
            }
            
            @Override
            public void run() {
                delegate.run();
            }
            
            @Override
            protected ListenableFuture<Long> delegate() {
                return delegate;
            }
        }
        
        protected final class NodeCreated extends Change {
        
            protected NodeCreated(
                    ZNodePath fromPath,
                    ListenableFuture<?>...chain) {
                super(fromPath, chain);
            }
            
            @Override
            public Optional<? extends ListenableFuture<?>> apply(
                    FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
                switch (input.size()) {
                case 0:
                {
                    return Optional.of(walker.apply(fromPath));
                }
                default:
                    break;
                }
                return Optional.absent();
            }
        }
        
        protected final class NodeDataChanged extends Change {
        
            protected NodeDataChanged(
                    ZNodePath fromPath,
                    ListenableFuture<?>...chain) {
                super(fromPath, chain);
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public Optional<? extends ListenableFuture<?>> apply(
                    FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
                switch (input.size()) {
                case 0:
                {
                    return Optional.of(getFromData.apply(fromPath));
                }
                case 1:
                {
                    O response = (O) ((List<?>) input.getLast().get()).get(1);
                    ListenableFuture<Long> future;
                    if (!Operations.maybeError(response.record(), KeeperException.Code.NONODE).isPresent()) {
                        final IGetDataResponse getData = (IGetDataResponse) response.record();
                        future = Futures.transform(
                                UnlessSequentialError.create(
                                        toClient.submit(
                                                StampedValue.valueOf(
                                                    Stats.CreateStat.ephemeralOwnerNone(),
                                                    Operations.Requests.setData()
                                                    .setPath(fromPath)
                                                    .setData(getData.getData())))), 
                                    Functions.constant(Long.valueOf(response.zxid())));
                    } else {
                        NodeDeleted delegate = new NodeDeleted(
                                fromPath, input.get(0));
                        delegate.run();
                        future = delegate;
                    }
                    return Optional.of(future);
                }
                default:
                    break;
                }
                return Optional.absent();
            }
        }

        protected final class NodeChildrenChanged extends Change {

            protected NodeChildrenChanged(
                    ZNodePath fromPath,
                    ListenableFuture<?>...chain) {
                super(fromPath, chain);
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public Optional<? extends ListenableFuture<?>> apply(
                    FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
                switch (input.size()) {
                case 0:
                {
                    return Optional.of(
                            Futures.allAsList(
                                    getFromChildren.apply(fromPath),
                                    Futures.allAsList(
                                            toClient.submit(
                                                StampedValue.valueOf(
                                                    Stats.CreateStat.ephemeralOwnerNone(),
                                                    Operations.Requests.sync()
                                                    .setPath(fromPath))),
                                            toClient.submit(
                                                    StampedValue.valueOf(
                                                        Stats.CreateStat.ephemeralOwnerNone(),
                                                        Operations.Requests.getChildren()
                                                        .setPath(fromPath))))));
                }
                case 1:
                {
                    List<?> last = (List<?>) input.getLast().get();
                    O fromResponse = (O) ((List<?>) last.get(0)).get(1);
                    O toResponse = (O) ((Pair<?,?>) ((List<?>) last.get(0)).get(1)).second();
                    ListenableFuture<?> future;
                    if (!Operations.maybeError(fromResponse.record(), KeeperException.Code.NONODE).isPresent()) {
                        // we only create new children
                        // because any deleted children should generate a NodeDeleted event
                        // which we'll handle separately
                        List<ZNodeName> toWalk = ImmutableList.of();
                        if (!Operations.maybeError(toResponse.record(), KeeperException.Code.NONODE).isPresent()) {
                            final List<String> fromChildren = ((Records.ChildrenGetter) fromResponse).getChildren();
                            final Set<String> toChildren = ImmutableSet.copyOf(((Records.ChildrenGetter) toResponse).getChildren());
                            for (String fromChild: fromChildren) {
                                if (!toChildren.contains(fromChild)) {
                                    if (toWalk.isEmpty()) {
                                        toWalk = Lists.newArrayListWithCapacity(fromChildren.size());
                                    }
                                    toWalk.add(ZNodeLabel.fromString(fromChild));
                                }
                            }
                        } else {
                            toWalk = ImmutableList.<ZNodeName>of(EmptyZNodeLabel.getInstance());
                        }
                        if (toWalk.isEmpty()) {
                            future = Futures.immediateFuture(Long.valueOf(fromResponse.zxid()));
                        } else {
                            List<ListenableFuture<Long>> futures = Lists.newArrayListWithCapacity(toWalk.size());
                            for (ZNodeName name: toWalk) {
                                futures.add(walker.apply(fromPath.join(name)));
                            }
                            future = Futures.allAsList(futures);
                        }
                    } else {
                        if (!Operations.maybeError(toResponse.record(), KeeperException.Code.NONODE).isPresent()) {
                            NodeDeleted delegate = new NodeDeleted(
                                    fromPath, Futures.immediateFuture(last.get(0)));
                            delegate.run();
                            future = delegate;
                        } else {
                            future = Futures.immediateFuture(Long.valueOf(fromResponse.zxid()));
                        }
                    }
                    return Optional.of(future);
                }
                case 2:
                {
                    Object last = input.getLast().get();
                    if (last instanceof List<?>) {
                        Long max = Long.valueOf(-1L);
                        for (Long result: (List<Long>) last) {
                            if (result.longValue() > max.longValue()) {
                                max = result;
                            }
                        }
                        assert (max.longValue() > 0L);
                        return Optional.of(Futures.immediateFuture(max));
                    }
                }
                default:
                    break;
                }
                return Optional.absent();
            }
        }
        
        protected final class NodeDeleted extends Change {

            protected NodeDeleted(
                    ZNodePath fromPath,
                    ListenableFuture<?>...chain) {
                super(fromPath, chain);
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public Optional<? extends ListenableFuture<?>> apply(
                    FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
                switch (input.size()) {
                case 0:
                {
                    return Optional.of(
                            Futures.allAsList(
                                    getFromData.apply(fromPath),
                                    MaybeSequentialError.create(
                                            toClient.submit(StampedValue.valueOf(
                                                    Stats.CreateStat.ephemeralOwnerNone(),
                                                    Operations.Requests.delete().setPath(fromPath))),
                                            KeeperException.Code.NONODE)));
                }
                case 1:
                {
                    List<?> last = (List<?>) input.getLast().get();
                    O fromResponse = (O) ((List<?>) last.get(0)).get(1);
                    ListenableFuture<Long> future;
                    if (!Operations.maybeError(fromResponse.record(), KeeperException.Code.NONODE).isPresent()) {
                        future = walker.apply(fromPath);
                    } else {
                        future = Futures.immediateFuture(Long.valueOf(fromResponse.zxid()));
                    }
                    return Optional.of(future);
                }
                default:
                    break;
                }
                return Optional.absent();
            }
        }
    }
    
    protected static final class EphemeralsSnapshotter<O extends Operation.ProtocolResponse<?>> implements TaskExecutor<StampedValue<? extends Operations.PathBuilder<? extends Records.Request,?>>, Pair<ZNodePath,O>> {

        public static <O extends Operation.ProtocolResponse<?>> EphemeralsSnapshotter<O> create(
                final ZNodePath ephemeralsPrefix,
                final ZNodePath rootPrefix,
                final AsyncFunction<Long, Long> sessions,
                final AsyncFunction<ZNodePath, ZNodePath> paths,
                final TaskExecutor<? super Records.Request, O> client) {
            return new EphemeralsSnapshotter<O>(
                    ephemeralsPrefix,
                    new Function<ZNodePath,ZNodeLabel>() {
                        @Override
                        public ZNodeLabel apply(ZNodePath input) {
                            return ZNodeLabel.fromString(EscapedConverter.getInstance().convert(
                                            input.suffix(rootPrefix).toString()));
                        }
                    },
                    sessions, 
                    paths, 
                    client);
        }
        
        protected final TaskExecutor<? super Records.Request, O> client;
        protected final AsyncFunction<ZNodePath, ZNodePath> paths;
        protected final Function<ZNodePath,ZNodeLabel> labelOf;
        protected final SessionPrefix sessionPrefix;
        protected final ConcurrentMap<Long, SessionEphemeralSnapshotter> ephemerals;
        protected final Logger logger;
        
        protected EphemeralsSnapshotter(
                final ZNodePath ephemeralsPrefix,
                final Function<ZNodePath,ZNodeLabel> labelOf,
                final AsyncFunction<Long, Long> sessions,
                final AsyncFunction<ZNodePath, ZNodePath> paths,
                final TaskExecutor<? super Records.Request, O> client) {
            this.labelOf = labelOf;
            this.client = client;
            this.paths = paths;
            this.sessionPrefix = new SessionPrefix(ephemeralsPrefix, sessions);
            this.ephemerals = new MapMaker().makeMap();
            this.logger = LogManager.getLogger(this);
        }
        
        @Override
        public ListenableFuture<Pair<ZNodePath,O>> submit(StampedValue<? extends Operations.PathBuilder<? extends Records.Request,?>> request) {
            Long session = Long.valueOf(request.stamp());
            SessionEphemeralSnapshotter ephemeral = ephemerals.get(session);
            if (ephemeral == null) {
                ListenableFuture<Pair<Long,ZNodePath>> prefix = sessionPrefix.submit(Long.valueOf(
                        session));
                ephemeral = new SessionEphemeralSnapshotter(session, prefix);
                if (ephemerals.putIfAbsent(session, ephemeral) != null) {
                    return submit(request);
                }
            }
            return ephemeral.submit(request.get());
        }
        
        public void stop() {
            for (SessionEphemeralSnapshotter v: Iterables.consumingIterable(ephemerals.values())) {
                v.stop();
            }
        }
        
        protected final class SessionPrefix implements TaskExecutor<Long, Pair<Long,ZNodePath>>, AsyncFunction<Long, Pair<Long,ZNodePath>> {

            protected final AsyncFunction<Long, Long> sessions;
            protected final ZNodePath prefix;
            
            public SessionPrefix(
                    ZNodePath prefix,
                    AsyncFunction<Long, Long> sessions) {
                this.prefix = prefix;
                this.sessions = sessions;
            }
            
            @Override
            public ListenableFuture<Pair<Long,ZNodePath>> submit(Long request) {
                try {
                    return Futures.transform(sessions.apply(request), this);
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }
            
            @Override
            public ListenableFuture<Pair<Long,ZNodePath>> apply(Long input) {
                if (input == null) {
                    return Futures.immediateFuture(null);
                }
                ZNodePath path = prefix.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.labelOf(input.longValue()));
                Operations.Requests.Create create = Operations.Requests.create();
                @SuppressWarnings("unchecked")
                ListenableFuture<List<O>> future = Futures.allAsList(
                        client.submit(create.setPath(path).build()),
                        client.submit(create.setPath(path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.LABEL)).build()));
                return Futures.transform(future, new Callback(Pair.create(input, create.getPath())));
            }
            
            protected final class Callback implements AsyncFunction<List<O>, Pair<Long,ZNodePath>> {
                private final Pair<Long,ZNodePath> value;
                
                public Callback(Pair<Long,ZNodePath> value) {
                    this.value = value;
                }
                
                @Override
                public ListenableFuture<Pair<Long,ZNodePath>> apply(List<O> input) throws Exception {
                    for (O response: input) {
                        Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS);
                    }
                    return Futures.immediateFuture(value);
                }
            }
        }
        
        protected final class SessionEphemeralSnapshotter extends ListenableFutureActor<Operations.PathBuilder<? extends Records.Request,?>,Pair<ZNodePath,O>,EphemeralSnapshotOperation> {
            
            protected final Long session;
            protected final ListenableFuture<Pair<Long,ZNodePath>> prefix;
            protected final ClassToInstanceMap<Operations.PathBuilder<? extends Records.Request,?>> operations;

            protected SessionEphemeralSnapshotter(
                    Long session,
                    ListenableFuture<Pair<Long,ZNodePath>> prefix) {
                super(Queues.<EphemeralSnapshotOperation>newConcurrentLinkedQueue(), 
                        EphemeralsSnapshotter.this.logger);
                this.session = session;
                this.prefix = prefix;
                this.operations = ImmutableClassToInstanceMap.<Operations.PathBuilder<? extends Records.Request,?>>builder()
                        .put(Operations.Requests.Create.class, Operations.Requests.create())
                        .put(Operations.Requests.Delete.class, Operations.Requests.delete())
                        .put(Operations.Requests.SetData.class, Operations.Requests.setData())
                        .build();
                prefix.addListener(this, MoreExecutors.directExecutor());
            }

            @Override
            public boolean isReady() {
                return prefix.isDone() && super.isReady();
            }

            @Override
            public ListenableFuture<Pair<ZNodePath,O>> submit(Operations.PathBuilder<? extends Records.Request,?> request) {
                final Operations.PathBuilder<? extends Records.Request, ?> operation = operations.get(request.getClass());
                checkArgument(operation != null);
                ListenableFuture<ZNodePath> future;
                try {
                    future = paths.apply(request.getPath());
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                final Promise<Pair<ZNodePath,O>> promise = SettableFuturePromise.create();
                final EphemeralSnapshotOperation task = new EphemeralSnapshotOperation(request, operation, future, promise);
                if (!send(task)) {
                    throw new RejectedExecutionException();
                }
                return promise;
            }

            @Override
            public void run() {
                super.run();
                if (prefix.isDone() && mailbox.isEmpty()) {
                    try {
                        if (prefix.get() == null) {
                            ephemerals.remove(session, this);
                        }
                    } catch (Exception e) {
                    }
                }
            }
            
            @Override
            protected boolean doApply(EphemeralSnapshotOperation input)
                    throws Exception {
                if (prefix.isCancelled()) {
                    input.promise.cancel(false);
                    return true;
                }
                ZNodePath prefix;
                try {
                    prefix = this.prefix.get().second();
                } catch (Exception e) {
                    input.promise.setException(e);
                    return true;
                }
                if (prefix == null) {
                    // session expired
                    input.promise.set(null);
                    return true;
                }
                try {
                    ZNodePath path = input.get();
                    if (input.operation instanceof Operations.DataBuilder<?,?>) {
                        Operations.Requests.Create create;
                        if (input.request instanceof Operations.Requests.Create) {
                            create = (Operations.Requests.Create) input.request;
                            create.setMode(create.getMode().ephemeral());
                        } else {
                            create = Operations.Requests.create()
                                    .setMode(CreateMode.EPHEMERAL)
                                    .setData(((Operations.Requests.SetData) input.request).getData());
                        }
                        Optional<? extends Sequential<String,?>> sequential = Sequential.maybeFromString(path.label().toString());
                        if (sequential.isPresent()) {
                            create.setMode(create.getMode().sequential())
                            .setPath(((AbsoluteZNodePath) path).parent().join(ZNodeLabel.fromString(sequential.get().prefix())));
                        } else {
                            create.setPath(path);
                        }
                        ByteBufOutputArchive buf = new ByteBufOutputArchive(Unpooled.buffer());
                        Records.Requests.serialize(create.build(), buf);
                        ((Operations.DataBuilder<?,?>) input.operation).setData(buf.get().array());
                    }
                    Callback.create(
                            client.submit(
                                    input.operation.setPath(prefix.join(labelOf.apply(path))).build()), 
                            PromiseTask.of(path, input.promise));
                } catch (Exception e) {
                    input.promise.setException(e);
                } finally {
                    input.operation.setPath(prefix);
                    if (input.operation instanceof Operations.DataBuilder<?,?>) {
                        ((Operations.DataBuilder<?,?>) input.operation).setData(new byte[0]);
                    }
                }
                return true;
            }
            
            @Override
            protected void doStop() {
                EphemeralSnapshotOperation next;
                while ((next = mailbox.poll()) != null) {
                    next.promise.cancel(false);
                }
            }
        }
        
        protected final class EphemeralSnapshotOperation extends ToStringListenableFuture<ZNodePath> {
 
            protected final Operations.PathBuilder<? extends Records.Request,?> request;
            protected final Operations.PathBuilder<? extends Records.Request,?> operation;
            protected final ListenableFuture<ZNodePath> future;
            protected final Promise<Pair<ZNodePath,O>> promise;
            
            protected EphemeralSnapshotOperation(
                    Operations.PathBuilder<? extends Records.Request,?> request,
                    Operations.PathBuilder<? extends Records.Request,?> operation,
                    ListenableFuture<ZNodePath> future,
                    Promise<Pair<ZNodePath,O>> promise) {
                this.request = request;
                this.future = future;
                this.operation = operation;
                this.promise = promise;
            }
            
            @Override
            protected ListenableFuture<ZNodePath> delegate() {
                return future;
            }
        }
        
        protected static final class Callback<T, O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<O> implements Runnable {

            public static <T,O extends Operation.ProtocolResponse<?>> Callback<T,O> create(
                    ListenableFuture<O> future,
                    PromiseTask<T,Pair<T,O>> promise) {
                Callback<T,O> callback = new Callback<T,O>(future, promise);
                future.addListener(callback, MoreExecutors.directExecutor());
                return callback;
            }
            
            private final PromiseTask<T,Pair<T,O>> promise;
            
            protected Callback(
                    ListenableFuture<O> future,
                    PromiseTask<T,Pair<T,O>> promise) {
                super(future);
                this.promise = promise;
            }
            
            @Override
            public void run() {
                if (isDone() && !promise.isDone()) {
                    if (isCancelled()) {
                        promise.cancel(false);
                    } else {
                        try {
                            O response = get();
                            Operations.unlessError(response.record());
                            promise.set(Pair.create(promise.task(), response));
                        } catch (Exception e) {
                            promise.setException(e);
                        }
                    }
                }
            }
        }
    }

    protected static final class UnlessError<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<O> implements Callable<Optional<O>> {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<O> create(ListenableFuture<O> future) {
            return CallablePromiseTask.listen(new UnlessError<O>(future), SettableFuturePromise.<O>create());
        }
        
        protected UnlessError(ListenableFuture<O> delegate) {
            super(delegate);
        }

        @Override
        public Optional<O> call() throws Exception {
            if (isDone()) {
                O response = get();
                Operations.unlessError(response.record());
                return Optional.of(response);
            }
            return Optional.absent();
        }
    }
    
    protected static final class GetFirst<T> implements Function<Pair<? extends T,?>,T> {

        public GetFirst() {}
        
        @Override
        public T apply(Pair<? extends T, ?> input) {
            return input.first();
        }
    }
    
    protected static final class GetSecond<T> implements Function<Pair<?,? extends T>,T> {

        public GetSecond() {}
        
        @Override
        public T apply(Pair<?,? extends T> input) {
            return input.second();
        }
    }
    
    protected static final class MaybeSequentialError<T,O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<Pair<T,O>> implements Callable<Optional<Pair<T,O>>> {
        
        public static <T,O extends Operation.ProtocolResponse<?>> ListenableFuture<Pair<T,O>> create(
                ListenableFuture<Pair<T,O>> delegate,
                KeeperException.Code...codes) {
            return CallablePromiseTask.listen(new MaybeSequentialError<T,O>(codes, delegate), SettableFuturePromise.<Pair<T,O>>create());
        }
        
        private final KeeperException.Code[] codes;
        
        protected MaybeSequentialError(
                KeeperException.Code[] codes,
                ListenableFuture<Pair<T,O>> delegate) {
            super(delegate);
            this.codes = codes;
        }
        
        @Override
        public Optional<Pair<T,O>> call() throws Exception {
            if (isDone()) {
                Pair<T,O> value = get();
                Operations.maybeError(value.second().record(), codes);
                return Optional.of(value);
            }
            return Optional.absent();
        }
    }
    
    protected static final class UnlessSequentialError<T,O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<Pair<T,O>> implements Callable<Optional<Pair<T,O>>> {
        
        public static <T,O extends Operation.ProtocolResponse<?>> ListenableFuture<Pair<T,O>> create(
                ListenableFuture<Pair<T,O>> delegate) {
            return CallablePromiseTask.listen(new UnlessSequentialError<T,O>(delegate), SettableFuturePromise.<Pair<T,O>>create());
        }
        
        protected UnlessSequentialError(
                ListenableFuture<Pair<T,O>> delegate) {
            super(delegate);
        }
        
        @Override
        public Optional<Pair<T,O>> call() throws Exception {
            if (isDone()) {
                Pair<T,O> value = get();
                Operations.unlessError(value.second().record());
                return Optional.of(value);
            }
            return Optional.absent();
        }
    }
    
    
    /**
     * Interposes on requests to maintain a namespace mapping for sequential znodes.
     */
    protected static final class SequentialTranslator<O extends Operation.ProtocolResponse<?>> implements TaskExecutor<Operations.PathBuilder<? extends Records.Request,?>, Pair<ZNodePath,O>>, AsyncFunction<ZNodePath, ZNodePath> {

        public static <O extends Operation.ProtocolResponse<?>> SequentialTranslator<O> create(
                ZNodePath fromPrefix,
                ZNodePath toPrefix,
                ClientExecutor<? super Records.Request, O, SessionListener> client) {
            return new SequentialTranslator<O>(
                    fromPrefix,
                    toPrefix,
                    client);
        }
        
        private final Logger logger;
        private final ZNodePath fromPrefix;
        private final SimpleNameTrie<ValueNode<SequentialZNode>> trie;
        private final ClientExecutor<? super Records.Request, O, SessionListener> client;
        
        protected SequentialTranslator(
                ZNodePath fromPrefix,
                ZNodePath toPrefix,
                ClientExecutor<? super Records.Request, O, SessionListener> client) {
            this.logger = LogManager.getLogger(this);
            this.fromPrefix = fromPrefix;
            this.trie = SimpleNameTrie.forRoot(
                            ValueNode.root(
                                    new SequentialZNode(
                                            Futures.immediateFuture(toPrefix))));
            this.client = client;
        }

        @Override
        public ListenableFuture<ZNodePath> apply(
                final ZNodePath input) throws Exception {
            Lookup lookup = new Lookup(input);
            lookup.run();
            return lookup;
        }

        @Override
        public ListenableFuture<Pair<ZNodePath,O>> submit(Operations.PathBuilder<? extends Records.Request,?> request) {
            RequestAction<?> action;
            if (request instanceof Operations.Requests.Create) {
                action = new Create((Operations.Requests.Create) request);
            } else if (request instanceof Operations.Requests.Delete) {
                // disallow deleting the root
                if (request.getPath().length() == fromPrefix.length()) {
                    throw new RejectedExecutionException(new KeeperException.NoNodeException());
                }
                action = new Delete((Operations.Requests.Delete) request);
            } else if (request instanceof Operations.Requests.GetChildren) {
                action = new GetChildren((Operations.Requests.GetChildren) request);
            } else {
                action = new NonCreateDelete<Operations.PathBuilder<? extends Records.Request,?>>(request);
            }
            action.run();
            return action;
        }
        
        protected abstract class Action<T,V> extends ForwardingPromise<V> implements Runnable {
            
            protected final PromiseTask<T,V> delegate;
            protected Iterator<ZNodeName> sequences;
            protected ValueNode<SequentialZNode> node;
            
            protected Action(
                    T task) {
                this(PromiseTask.<T,V>of(task));
            }
            
            protected Action(
                    PromiseTask<T,V> delegate) {
                this.delegate = delegate;
            }
            
            public abstract ZNodeName fromName();

            @Override
            public void run() {
                synchronized (trie) {
                    while (!isDone()) {
                        try {
                            if (!iterate()) {
                                break;
                            }
                        } catch (Exception e) {
                            setException(e);
                        }
                    }
                }
            }
            
            public void reset() {
                synchronized (trie) {
                    this.node = trie.root();
                    this.sequences = SequenceIterator.forName(fromName());
                }
            }
            
            protected abstract boolean iterate() throws Exception;
            
            @Override
            protected PromiseTask<T,V> delegate() {
                return delegate;
            }
        }
        
        protected final class SequentialZNode extends AbstractPair<ListenableFuture<ZNodePath>, Deque<Action<?,?>>> implements Runnable {
        
            protected SequentialZNode(
                    ListenableFuture<ZNodePath> toPath) {
                this(toPath, ImmutableList.<Action<?,?>>of());
            }
            
            protected SequentialZNode(
                    ListenableFuture<ZNodePath> toPath,
                    Iterable<? extends Action<?,?>> dependents) {
                super(toPath, Lists.<Action<?,?>>newLinkedList(dependents));
            }
            
            public ListenableFuture<ZNodePath> toPath() {
                return first;
            }
            
            public Deque<Action<?,?>> dependents() {
                return second;
            }
            
            @Override
            public void run() {
                synchronized (trie) {
                    Runnable next;
                    while ((next = dependents().peek()) != null) {
                        next.run();
                        if (next == dependents().peek()) {
                            break;
                        }
                    }
                }
            }
        }

        protected final class Lookup extends Action<ZNodePath,ZNodePath> {

            protected Lookup(ZNodePath task) {
                super(task);
                reset();
            }

            @Override
            public ZNodeName fromName() {
                return delegate().task().suffix(fromPrefix);
            }

            @Override
            protected boolean iterate() throws Exception {
                if (!node.get().toPath().isDone() || !node.get().dependents().isEmpty()) {
                    if (!node.get().dependents().contains(this)) {
                        node.get().dependents().add(this);
                        return false;
                    }
                    if (!node.get().toPath().isDone() || (this != node.get().dependents().peek())) {
                        return false;
                    } else {
                        node.get().dependents().remove(this);
                    }
                }
                ZNodePath path = node.get().toPath().get();
                ZNodeName remaining;
                if (sequences.hasNext()) {
                    remaining = sequences.next();
                    ValueNode<SequentialZNode> next = node.get(remaining);
                    if (next != null) {
                        node = next;
                        return true;
                    }
                    if (sequences.hasNext()) {
                        throw new KeeperException.NoNodeException();
                    }
                    path = path.join(remaining);
                } else {
                    remaining = EmptyZNodeLabel.getInstance();
                }
                set(path);
                return false;
            }
        }
        
        protected abstract class RequestAction<T extends Operations.PathBuilder<? extends Records.Request,?>> extends Action<T,Pair<ZNodePath,O>> {
            
            protected final ZNodePath fromPath;
            protected ListenableFuture<Pair<ZNodePath,O>> future;
            
            protected RequestAction(
                    T task) {
                super(task);
                this.future = null;
                this.fromPath = task.getPath();
                reset();
            }
            
            @Override
            public ZNodeName fromName() {
                return fromPath.suffix(fromPrefix);
            }

            @Override
            public void run() {
                synchronized (trie) {
                    do {
                        try {
                            if (!iterate()) {
                                break;
                            }
                        } catch (Exception e) {
                            if (!isDone()) {
                                setException(e);
                            }
                        }
                    } while (true);
                }
            }
            
            public void reset() {
                synchronized (trie) {
                    this.node = trie.root();
                    this.sequences = SequenceIterator.forName(fromName());
                }
            }
            
            protected ListenableFuture<Pair<ZNodePath,O>> submit(
                    Processor<? super O, ? extends ZNodePath> toPath) {
                return Response.create(
                        toPath,
                        client.submit(delegate().task().build()),
                        delegate());
            }
        }
        
        protected abstract class NonDelete<T extends Operations.PathBuilder<? extends Records.Request,?>> extends RequestAction<T> {

            protected NonDelete(
                    T task) {
                super(task);
            }

            @Override
            protected boolean iterate() throws Exception {
                if (isDone()) {
                    return false;
                }
                if (!node.get().toPath().isDone() || !node.get().dependents().isEmpty()) {
                    if (!node.get().dependents().contains(this)) {
                        node.get().dependents().add(this);
                        return false;
                    }
                    if (!node.get().toPath().isDone() || (this != node.get().dependents().peek())) {
                        return false;
                    } else {
                        node.get().dependents().remove(this);
                    }
                }
                ZNodePath path = node.get().toPath().get();
                ZNodeName remaining;
                if (sequences.hasNext()) {
                    remaining = sequences.next();
                    ValueNode<SequentialZNode> next = node.get(remaining);
                    if (next != null) {
                        node = next;
                        return true;
                    }
                    if (sequences.hasNext()) {
                        throw new KeeperException.NoNodeException();
                    }
                    path = path.join(remaining);
                } else {
                    remaining = EmptyZNodeLabel.getInstance();
                }
                future = submit(path, remaining);
                return false;
            }
            
            protected abstract ListenableFuture<Pair<ZNodePath,O>> submit(ZNodePath path, ZNodeName remaining);
        }

        protected final class Create extends NonDelete<Operations.Requests.Create> {
            
            protected Create(Operations.Requests.Create task) {
                super(task);
            }

            @Override
            protected boolean iterate() throws Exception {
                if (future != null) {
                    if (future.isDone()) {
                        assert (isDone());
                        if (this == node.get().dependents().peek()) {
                            node.get().dependents().remove(this);
                            try {
                                Pair<ZNodePath,O> result = future.get();
                                Operations.unlessError(result.second().record());
                                logger.info("Snapshotted sequential {} => {}", fromPath, result.first());
                            } catch (Exception e) {
                                if (node.remove()) {
                                    Action<?,?> next;
                                    while ((next = node.get().dependents().poll()) != null) {
                                        if (future.isCancelled()) {
                                            next.cancel(false);
                                        } else {
                                            next.setException(e);
                                        }
                                    }
                                } else {
                                    throw new ConcurrentModificationException();
                                }
                            }
                        }
                    }
                    return false;
                }
                return super.iterate();
            }
            
            @Override
            protected ListenableFuture<Pair<ZNodePath,O>> submit(ZNodePath path, ZNodeName remaining) {
                if (remaining instanceof EmptyZNodeLabel) {
                    // Note that we don't check that the mode, owner, or data
                    // is what it's supposed to be
                    return Futures.immediateFuture(Pair.create(path, (O) null));
                }
                Operations.Requests.Create create = delegate().task();
                Optional<? extends Sequential<String,?>> sequential = Sequential.maybeFromString(path.label().toString());
                ListenableFuture<Pair<ZNodePath,O>> future;
                if (sequential.isPresent()) {
                    create.setMode(create.getMode().sequential());
                    create.setPath((((AbsoluteZNodePath) path).parent().join(ZNodeLabel.fromString(sequential.get().prefix()))));
                    future = submit(FromPathGetter.create(path));
                    final SequentialZNode znode = new SequentialZNode(
                            Futures.transform(future, GET_PATH),
                            ImmutableList.of(this));
                    final ValueNode<SequentialZNode> child = ValueNode.child(
                                    znode, 
                                    remaining, 
                                    node);
                    node.put(remaining, child);
                    node = child;
                    znode.toPath().addListener(znode, MoreExecutors.directExecutor());
                } else {
                    create.setPath(path);
                    future = submit(FromPathGetter.create(path));
                }
                return future;
            }
        }
        
        protected class NonCreateDelete<T extends Operations.PathBuilder<? extends Records.Request,?>> extends NonDelete<T> {
        
            protected NonCreateDelete(
                    T task) {
                super(task);
            }

            @Override
            protected boolean iterate() throws Exception {
                if (future != null) {
                    return false;
                }
                return super.iterate();
            }
            
            @Override
            protected ListenableFuture<Pair<ZNodePath,O>> submit(ZNodePath path, ZNodeName remaining) {
                delegate().task().setPath(path);
                return submit(Processors.constant(path));
            }
        }
        
        protected final class GetChildren extends NonCreateDelete<Operations.Requests.GetChildren> {
        
            protected GetChildren(
                    Operations.Requests.GetChildren task) {
                super(task);
            }
            
            @Override
            protected ListenableFuture<Pair<ZNodePath,O>> submit(ZNodePath path, ZNodeName remaining) {
                final ZNodeName fromName = fromName();
                return Futures.transform(super.submit(path, remaining), new ChildrenTranslator(fromName));
            }
            
            protected final class ChildrenTranslator implements Function<Pair<ZNodePath,O>,Pair<ZNodePath,O>> {
                
                private final ZNodeName fromName;
                
                protected ChildrenTranslator(
                        ZNodeName fromName) {
                    this.fromName = fromName;
                }

                @Override
                public Pair<ZNodePath, O> apply(Pair<ZNodePath, O> input) {
                    if (input.second().record() instanceof Records.ChildrenGetter) {
                        synchronized (trie) {
                            ValueNode<SequentialZNode> node = trie.root();
                            Iterator<ZNodeName> sequences = SequenceIterator.forName(fromName);
                            ZNodeName name;
                            do {
                                name = sequences.next();
                                ValueNode<SequentialZNode> next = node.get(name);
                                if (next != null) {
                                    node = next;
                                    name = EmptyZNodeLabel.getInstance();
                                } else {
                                    break;
                                }
                            } while (sequences.hasNext());

                            // TODO should we check for deleted children?
                            
                            Map<String, String> fromLabels = ImmutableMap.of();
                            for (Map.Entry<ZNodeName, ValueNode<SequentialZNode>> entry: node.entrySet()) {
                                Optional<ZNodeLabel> label = Optional.absent();
                                if (name instanceof EmptyZNodeLabel) { 
                                    if (entry.getKey() instanceof ZNodeLabel) {
                                        label = Optional.of((ZNodeLabel) entry.getKey());
                                    }
                                } else {
                                    if (entry.getKey().startsWith(name)) {
                                        ZNodeName remaining = ((RelativeZNodePath) entry.getKey()).suffix(name.length());
                                        if (remaining instanceof ZNodeLabel) {
                                            label = Optional.of((ZNodeLabel) remaining);
                                        } else {
                                            assert (! (remaining instanceof EmptyZNodeLabel));
                                        }
                                    }
                                }
                                if (label.isPresent()) {
                                    if (fromLabels.isEmpty()) {
                                        fromLabels = Maps.newHashMapWithExpectedSize(node.size());
                                    }
                                    assert (entry.getValue().get().toPath().isDone());
                                    try {
                                        fromLabels.put(entry.getValue().get().toPath().get(0L, TimeUnit.SECONDS).label().toString(), label.get().toString());
                                    } catch (Exception e) {
                                        throw new UnsupportedOperationException(e);
                                    }
                                }
                            }

                            // This is not ideal, but we break the layers of abstraction
                            // by assuming that the underlying record list is mutable
                            // instead of creating a new record
                            // we do this because O is a type parameter and so
                            // not instantiable
                            List<String> children = ((Records.ChildrenGetter) input.second().record()).getChildren();
                            ListIterator<String> itr = children.listIterator();
                            while (itr.hasNext()) {
                                String child = itr.next();
                                String translated = fromLabels.get(child);
                                if (translated != null) {
                                    itr.set(translated);
                                }
                            }
                        }
                    }
                    return input;
                }
            }
        }

        protected final class Delete extends RequestAction<Operations.Requests.Delete> {

            protected Delete(Operations.Requests.Delete task) {
                super(task);
            }
            
            @Override
            protected boolean iterate() throws Exception {
                if (future != null) {
                    if (future.isDone()) {
                        assert (isDone());
                        if (this == node.get().dependents().peek()) {
                            node.get().dependents().remove(this);
                            try {
                                Pair<ZNodePath,O> result = future.get();
                                Operations.maybeError(result.second().record(), KeeperException.Code.NONODE);
                                if (node.remove()) {
                                    Action<?,?> next;
                                    while ((next = node.get().dependents().poll()) != null) {
                                        next.reset();
                                        next.run();
                                    }
                                    node.parent().get().get().run();
                                } else {
                                    throw new ConcurrentModificationException();
                                }
                            } finally {
                                node.get().run();
                            }
                        }
                    }
                    return false;
                }
                if (!node.get().toPath().isDone() || !node.get().dependents().isEmpty()) {
                    if (!node.get().dependents().contains(this)) {
                        node.get().dependents().add(this);
                        return false;
                    }
                    if (!node.get().toPath().isDone() || (this != node.get().dependents().peek())) {
                        return false;
                    }
                }
                ZNodePath path = node.get().toPath().get();
                if (sequences.hasNext()) {
                    ZNodeName name = sequences.next();
                    ValueNode<SequentialZNode> next = node.get(name);
                    if (next != null) {
                        if (this == node.get().dependents().peek()) {
                            node.get().dependents().remove(this);
                        }
                        node = next;
                        return true;
                    }
                    if (sequences.hasNext()) {
                        throw new KeeperException.NoNodeException();
                    }
                    for (ZNodeName k: node.keySet()) {
                        if (k.startsWith(name)) {
                            if (!node.get().dependents().contains(this)) {
                                node.get().dependents().add(this);
                            }
                            sequences = ImmutableList.of(name).iterator();
                            return false;
                        }
                    }
                    if (this == node.get().dependents().peek()) {
                        node.get().dependents().remove(this);
                    }
                    path = path.join(name);
                } else {
                    if (this != node.get().dependents().peek()) {
                        node.get().dependents().addFirst(this);
                    }
                }
                delegate().task().setPath(path);
                future = submit(Processors.constant(path));
                future.addListener(node.get(), MoreExecutors.directExecutor());
                return false;
            }
        }
        
        protected static final Function<Pair<? extends ZNodePath,?>,ZNodePath> GET_PATH = new GetFirst<ZNodePath>();
        
        protected static final class Response<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<O> implements Callable<Optional<Pair<ZNodePath,O>>> {

            public static <O extends Operation.ProtocolResponse<?>> CallablePromiseTask<Response<O>,Pair<ZNodePath,O>> create(
                    Processor<? super O, ? extends ZNodePath> toPath,
                    ListenableFuture<O> future,
                    Promise<Pair<ZNodePath,O>> promise) {
                return CallablePromiseTask.listen(new Response<O>(toPath, future), promise);
            }

            private final Processor<? super O, ? extends ZNodePath> toPath;
            
            protected Response(
                    Processor<? super O, ? extends ZNodePath> toPath,
                    ListenableFuture<O> future) {
                super(future);
                this.toPath = toPath;
            }
            
            @Override
            public Optional<Pair<ZNodePath,O>> call() throws Exception {
                if (isDone()) {
                    O response = get();
                    return Optional.of(Pair.create((ZNodePath) toPath.apply(response), response));
                }
                return Optional.absent();
            }
        }
        
        protected static final class FromPathGetter implements Processor<Operation.ProtocolResponse<?>, ZNodePath> {

            public static FromPathGetter create(ZNodePath path) {
                return new FromPathGetter(path);
            }
            
            private final ZNodePath path;
            
            protected FromPathGetter(ZNodePath path) {
                this.path = path;
            }
            
            @Override
            public ZNodePath apply(Operation.ProtocolResponse<?> input) {
                if (input.record() instanceof Records.PathGetter) {
                    return ZNodePath.fromString(((Records.PathGetter) input.record()).getPath());
                } else {
                    return path;
                }
            }
        }
    }
    
    /**
     * Iterates over subsequences of a path ending in either a sequential label or the last label.
     */
    protected static final class SequenceIterator extends AbstractIterator<ZNodeName> {

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
