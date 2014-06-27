package edu.uw.zookeeper.safari.storage;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.Unpooled;

import java.net.InetSocketAddress;
import java.util.Collection;
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

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.TreeWalker;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ListenableFutureActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.SubmitActor;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
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
import edu.uw.zookeeper.safari.backend.PrefixTranslator;
import edu.uw.zookeeper.safari.storage.SnapshotEnsembleWatches.FilteredWchc;
import edu.uw.zookeeper.safari.storage.SnapshotEnsembleWatches.StringToWchc;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;


public class Snapshot<O extends Operation.ProtocolResponse<?>> implements Function<List<Snapshot.Action<?>>, Optional<? extends Snapshot.Action<?>>> {
    
    public static <O extends Operation.ProtocolResponse<?>> DeleteExistingSnapshot undo(
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient,
            Identifier toVolume,
            ZNodeName toBranch) {
        return DeleteExistingSnapshot.create(toClient, toVolume, toBranch);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<UnsignedLong> recover(
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient,
            Identifier fromVolume,
            ZNodeName fromBranch,
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient,
            Identifier toVolume,
            ZNodeName toBranch,
            Materializer<StorageZNode<?>,?> materializer,
            ServerInetAddressView server,
            EnsembleView<ServerInetAddressView> storage,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
        List<Snapshot.Action<?>> steps = Lists.newLinkedList();
        steps.add(undo(toClient, toVolume, toBranch));
        return create(
                fromClient, 
                fromVolume, 
                fromBranch,
                toClient, 
                toVolume, 
                toBranch,
                materializer, 
                server,
                storage,
                anonymous,
                steps);
    }
    
    protected static <O extends Operation.ProtocolResponse<?>> ListenableFuture<UnsignedLong> create(
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient,
            Identifier fromVolume,
            ZNodeName fromBranch,
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient,
            Identifier toVolume,
            ZNodeName toBranch,
            Materializer<StorageZNode<?>,?> materializer,
            ServerInetAddressView server,
            EnsembleView<ServerInetAddressView> storage,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous,
            List<Snapshot.Action<?>> steps) {
        final AbsoluteZNodePath fromRoot = StorageSchema.Safari.Volumes.Volume.Root.pathOf(fromVolume).join(fromBranch);
        final AbsoluteZNodePath toRoot = StorageSchema.Safari.Volumes.Volume.Root.pathOf(toVolume).join(toBranch);
        final PathTranslator paths = PathTranslator.empty(fromRoot, toRoot);
        final ZxidTracker zxid = ZxidTracker.zero();
        final Snapshot<O> snapshot = new Snapshot<O>(
                fromClient, 
                fromVolume, 
                fromBranch,
                fromRoot, 
                toClient, 
                toVolume, 
                toBranch, 
                toRoot, 
                materializer, 
                server,
                storage,
                anonymous,
                paths, 
                zxid);
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(snapshot, steps),
                        new Processor<List<Snapshot.Action<?>>, UnsignedLong>() {
                            @Override
                            public UnsignedLong apply(
                                    List<Action<?>> input)
                                    throws Exception {
                                for (int index=input.size()-1; index>=0; --index) {
                                    Object result = input.get(index).get();
                                    if (result instanceof UnsignedLong) {
                                        return (UnsignedLong) result;
                                    }
                                }
                                throw new AssertionError();
                            }
                        }), 
                SettableFuturePromise.<UnsignedLong>create());
    }
    
    protected final ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient;
    protected final Identifier fromVolume;
    protected final ZNodeName fromBranch;
    protected final ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient;
    protected final Identifier toVolume;
    protected final ZNodeName toBranch;
    protected final Materializer<StorageZNode<?>,?> materializer;
    protected final ServerInetAddressView server;
    protected final EnsembleView<ServerInetAddressView> storage;
    protected final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous;
    protected final AbsoluteZNodePath fromRoot;
    protected final AbsoluteZNodePath toRoot;
    protected final PathTranslator paths;
    protected final WalkerProcessor walker;
    
    protected Snapshot(
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient,
            Identifier fromVolume,
            ZNodeName fromBranch,
            AbsoluteZNodePath fromRoot,
            ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient,
            Identifier toVolume,
            ZNodeName toBranch,
            AbsoluteZNodePath toRoot,
            Materializer<StorageZNode<?>,?> materializer,
            ServerInetAddressView server,
            EnsembleView<ServerInetAddressView> storage,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous,
            PathTranslator paths,
            ZxidTracker zxid) {
        this.fromClient = fromClient;
        this.fromVolume = fromVolume;
        this.fromBranch = fromBranch;
        this.toClient = toClient;
        this.toVolume = toVolume;
        this.toBranch = toBranch;
        this.materializer = materializer;
        this.server = server;
        this.storage = storage;
        this.anonymous = anonymous;
        this.fromRoot = fromRoot;
        this.toRoot = toRoot;
        this.paths = paths;
        this.walker = new WalkerProcessor(zxid,
                SettableFuturePromise.<UnsignedLong>create());
    }
    
    @Override
    public Optional<? extends Action<?>> apply(List<Action<?>> input) {
        Optional<? extends ListenableFuture<?>> last = input.isEmpty() ? Optional.<ListenableFuture<?>>absent() : Optional.of(input.get(input.size()-1));
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
            return Optional.of(CreateSnapshotPrefix.create(toClient, toVolume));
        } else if (last.get() instanceof CreateSnapshotPrefix) {
            return Optional.of(
                    PrepareSnapshotEnsembleWatches.create(
                            StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.pathOf(toVolume),
                        new Function<ZNodePath,ZNodeLabel>() {
                            @Override
                            public ZNodeLabel apply(ZNodePath input) {
                                return ZNodeLabel.fromString(
                                        ESCAPE_ZNODE_NAME.apply(
                                                paths.apply(input).suffix(toRoot).toString()));
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
                                }, SameThreadExecutor.getInstance()),
                        materializer,
                        toClient,
                        storage,
                        anonymous,
                        new FromRootWchc(fromRoot)));
        } else if (last.get() instanceof PrepareSnapshotEnsembleWatches) {
            try {
                return Optional.of((CallSnapshotEnsembleWatches)
                        CallSnapshotEnsembleWatches.empty(
                                (PrepareSnapshotEnsembleWatches) last.get()));
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
        } else if (last.get() instanceof CallSnapshotEnsembleWatches) {
            Action<?> previous = input.get(input.size()-2);
            if (previous instanceof PrepareSnapshotEnsembleWatches) {
                final ListenableFuture<UnsignedLong> future = Futures.transform(
                        walker.walk(fromRoot), 
                        new WalkerCallback(), 
                        SameThreadExecutor.getInstance());
                return Optional.of(CreateSnapshot.create(future));
            } else if (previous instanceof CreateSnapshot) {
                try {
                    CallSnapshotEnsembleWatches.fromPrevious((CallSnapshotEnsembleWatches) last.get());
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
            } else {
                throw new AssertionError();
            }
        } else if (last.get() instanceof CreateSnapshot) {
            CallSnapshotEnsembleWatches watches = (CallSnapshotEnsembleWatches) input.get(input.size()-2);
            try {
                return Optional.of((CallSnapshotEnsembleWatches) CallSnapshotEnsembleWatches.fromPrevious(watches));
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
        }
        return Optional.absent();
    }
    
    public static final class FromRootWchc extends FilteredWchc {

        private final ZNodePath root;
        
        public FromRootWchc(ZNodePath root) {
            this.root = root;
        }
        
        @Override
        protected Iterable<ZNodePath> filter(Map.Entry<Long, Collection<ZNodePath>> entry) {
            ImmutableList.Builder<ZNodePath> filtered = ImmutableList.builder();
            for (ZNodePath path: entry.getValue()) {
                if (path.startsWith(root)) {
                    filtered.add(path);
                }
            }
            return filtered.build();
        }
    }
    
    public static final class WithoutSessionsWchc extends FilteredWchc {

        private final ImmutableSet<Long> sessions;
        
        public WithoutSessionsWchc(ImmutableSet<Long> sessions) {
            this.sessions = sessions;
        }
        
        @Override
        protected Iterable<ZNodePath> filter(Map.Entry<Long, Collection<ZNodePath>> entry) {
            if (!sessions.contains(entry.getKey())) {
                return entry.getValue();
            } else {
                return ImmutableSet.of();
            }
        }
    }
    
    protected static class WalkerCallback implements AsyncFunction<Optional<ListenableFuture<UnsignedLong>>, UnsignedLong> {

        public WalkerCallback() {}
        
        @Override
        public ListenableFuture<UnsignedLong> apply(
                Optional<ListenableFuture<UnsignedLong>> input)
                throws Exception {
            return input.get();
        }
    }
    
    public static abstract class Action<V> extends ToStringListenableFuture<V> {

        protected Action(ListenableFuture<V> delegate) {
            super(delegate);
        }
        
    }
    
    public static final class DeleteExistingSnapshot extends Action<List<List<AbsoluteZNodePath>>> {

        public static DeleteExistingSnapshot create(
                final ConnectionClientExecutor<? super Records.Request, ?, SessionListener, ?> toConnection,
                final Identifier toVolume,
                final ZNodeName toBranch) {
            ImmutableList<AbsoluteZNodePath> paths = 
                    ImmutableList.of(
                            StorageSchema.Safari.Volumes.Volume.Root.pathOf(toVolume).join(toBranch),
                            StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.pathOf(toVolume),
                            StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.pathOf(toVolume));
            ImmutableList.Builder<ListenableFuture<List<AbsoluteZNodePath>>> deletes = 
                    ImmutableList.builder();
            for (AbsoluteZNodePath path: paths) {
                deletes.add(DeleteSubtree.deleteChildren(path, toConnection));
            }
            return new DeleteExistingSnapshot(
                    Futures.allAsList(deletes.build()));
        }
        
        protected DeleteExistingSnapshot(
                ListenableFuture<List<List<AbsoluteZNodePath>>> future) {
            super(future);
        }
    }

    public static final class CreateSnapshotPrefix<O extends Operation.ProtocolResponse<?>> extends Action<List<O>> {
        
        public static <O extends Operation.ProtocolResponse<?>> CreateSnapshotPrefix<O> create(
                ConnectionClientExecutor<? super Records.Request,O,?,?> toConnection,
                Identifier toVolume) {
            ImmutableList<AbsoluteZNodePath> paths = 
                    ImmutableList.of(
                            StorageSchema.Safari.Volumes.Volume.pathOf(toVolume),
                            StorageSchema.Safari.Volumes.Volume.Root.pathOf(toVolume),
                            StorageSchema.Safari.Volumes.Volume.Snapshot.pathOf(toVolume),
                            StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.pathOf(toVolume),
                            StorageSchema.Safari.Volumes.Volume.Snapshot.Watches.pathOf(toVolume));
            ImmutableList.Builder<Records.Request> creates = 
                    ImmutableList.builder();
            for (AbsoluteZNodePath path: paths) {
                creates.add(Operations.Requests.create().setPath(path).build());
            }
            return new CreateSnapshotPrefix<O>(
                    SubmittedRequests.submit(toConnection, creates.build()));
        }
        
        protected CreateSnapshotPrefix(ListenableFuture<List<O>> future) {
            super(future);
        }
    }
    
    public static final class PrepareSnapshotEnsembleWatches extends Action<SnapshotEnsembleWatches> {
        
        public static <O extends Operation.ProtocolResponse<?>> PrepareSnapshotEnsembleWatches create(
                final ZNodePath prefix,
                final Function<ZNodePath,ZNodeLabel> labelOf,
                final ServerInetAddressView server,
                final ListenableFuture<Long> session,
                final Materializer<StorageZNode<?>,?> materializer,
                final ClientExecutor<? super Records.Request, O, SessionListener> client,
                final EnsembleView<ServerInetAddressView> ensemble,
                final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous,
                final Function<FourLetterWords.Wchc, FourLetterWords.Wchc> transformer) {
            final StringToWchc stringToWchc = new StringToWchc();
            final ListenableFuture<Function<FourLetterWords.Wchc, FourLetterWords.Wchc>> localTransformer = 
                    Futures.transform(
                            session, 
                            new Function<Long,Function<FourLetterWords.Wchc, FourLetterWords.Wchc>>() {
                                @Override
                                public Function<FourLetterWords.Wchc, FourLetterWords.Wchc> apply(Long input) {
                                    return new WithoutSessionsWchc(ImmutableSet.of(input));
                                }
                            }, SameThreadExecutor.getInstance());
            final Callable<Optional<SnapshotEnsembleWatches>> call = new Callable<Optional<SnapshotEnsembleWatches>>() {
                @Override
                public Optional<SnapshotEnsembleWatches> call()
                        throws Exception {
                    if (!localTransformer.isDone()) {
                        return Optional.absent();
                    }
                    ImmutableList.Builder<SnapshotEnsembleWatches.QueryServerWatches> servers = ImmutableList.builder();
                    for (ServerInetAddressView e: ensemble) {
                        final InetSocketAddress address = e.get();
                        final Supplier<? extends ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>> connection = new Supplier<ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>>() {
                            @Override
                            public ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> get() {
                                return anonymous.connect(address);
                            }
                        };
                        final Function<String, FourLetterWords.Wchc> transformWchc = Functions.compose(address.equals(server) ? Functions.compose(transformer, localTransformer.get()) : transformer, stringToWchc);
                        servers.add(SnapshotEnsembleWatches.QueryServerWatches.create(materializer, transformWchc, connection));
                    }
                    return Optional.of(SnapshotEnsembleWatches.forServers(prefix, labelOf, materializer.codec(), client, servers.build()));
                }
            };
            CallablePromiseTask<?, SnapshotEnsembleWatches> task = CallablePromiseTask.create(call, SettableFuturePromise.<SnapshotEnsembleWatches>create());
            localTransformer.addListener(task, SameThreadExecutor.getInstance());
            return new PrepareSnapshotEnsembleWatches(task);
        }
        
        protected PrepareSnapshotEnsembleWatches(
                ListenableFuture<SnapshotEnsembleWatches> delegate) {
            super(delegate);
        }
    }

    public static final class CallSnapshotEnsembleWatches extends Action<FourLetterWords.Wchc> {

        public static ListenableFuture<FourLetterWords.Wchc> empty(
                PrepareSnapshotEnsembleWatches prepare) throws InterruptedException, ExecutionException {
            return fromChain(ImmutableList.<ListenableFuture<FourLetterWords.Wchc>>of(), prepare);
        }

        public static ListenableFuture<FourLetterWords.Wchc> fromPrevious(
                CallSnapshotEnsembleWatches call) throws InterruptedException, ExecutionException {
            return fromChain(ImmutableList.<ListenableFuture<FourLetterWords.Wchc>>builder().addAll(call.chain()).add(call).build(), call.prepare());
        }
        
        public static ListenableFuture<FourLetterWords.Wchc> fromChain(
                List<ListenableFuture<FourLetterWords.Wchc>> chain,
                PrepareSnapshotEnsembleWatches prepare) throws InterruptedException, ExecutionException {
            SnapshotEnsembleWatches snapshot = prepare.get();
            Optional<? extends ListenableFuture<FourLetterWords.Wchc>> future = snapshot.apply(chain);
            if (future.isPresent()) {
                return new CallSnapshotEnsembleWatches(chain, prepare, future.get());
            } else {
                return chain.get(chain.size()-1);
            }
        }
        
        protected final List<ListenableFuture<FourLetterWords.Wchc>> chain;
        protected final PrepareSnapshotEnsembleWatches prepare;
        
        public CallSnapshotEnsembleWatches(
                List<ListenableFuture<FourLetterWords.Wchc>> chain,
                PrepareSnapshotEnsembleWatches prepare,
                ListenableFuture<FourLetterWords.Wchc> future) {
            super(future);
            this.chain = chain;
            this.prepare = prepare;
        }
        
        public List<ListenableFuture<FourLetterWords.Wchc>> chain() {
            return chain;
        }
        
        public PrepareSnapshotEnsembleWatches prepare() {
            return prepare;
        }
    }
    
    public static final class CreateSnapshot extends Action<UnsignedLong> {
        
        public static CreateSnapshot create(ListenableFuture<UnsignedLong> future) {
            return new CreateSnapshot(future);
        }
        
        protected CreateSnapshot(ListenableFuture<UnsignedLong> future) {
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
                final ZNodePath path = ZNodePath.fromString(((Records.PathGetter) input.request()).getPath());
                final ImmutableSortedSet<String> children = 
                        ImmutableSortedSet.copyOf(
                                Sequential.comparator(), 
                                ((Records.ChildrenGetter) response).getChildren());
                return Iterators.transform(
                                children.iterator(),
                                TreeWalker.ChildToPath.forParent(path));
            } else {
                return Iterators.emptyIterator();
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
    
    protected final class WalkerProcessor extends ForwardingListenableFuture<UnsignedLong> implements Processor<Optional<? extends SubmittedRequest<Records.Request,?>>, Optional<ListenableFuture<UnsignedLong>>>, Runnable, TaskExecutor<Pair<Optional<Long>,? extends Records.Request>, O> {

        protected final Promise<UnsignedLong> promise;
        protected final ToClient submitTo;
        protected final EphemeralZNodeLookups ephemerals;
        protected final FromListener changes;
        protected final TreeWalker.Builder<ListenableFuture<UnsignedLong>> walker;
        protected final ZxidTracker zxid;
        protected int walkers;
        
        public WalkerProcessor(
                ZxidTracker zxid,
                Promise<UnsignedLong> promise) {
            this.zxid = zxid;
            this.promise = promise;
            this.walkers = 0;
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
                        StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.pathOf(toVolume),
                        new Function<ZNodePath,ZNodeLabel>() {
                            @Override
                            public ZNodeLabel apply(ZNodePath input) {
                                return ZNodeLabel.fromString(
                                        ESCAPE_ZNODE_NAME.apply(
                                                input.suffix(toRoot).toString()));
                            }
                        }), this);
            this.changes = new FromListener();
            
            addListener(this, SameThreadExecutor.getInstance());
        }

        public synchronized TreeWalker<ListenableFuture<UnsignedLong>> walk(ZNodePath root) {
            ++walkers;
            return walker.setRoot(root).build();
        }
        
        @Override
        public synchronized void run() {
            if (!isDone()) {
                if ((walkers == 0) && submitTo.isEmpty() && ephemerals.isEmpty() && changes.isEmpty()) {
                    promise.set(UnsignedLong.valueOf(zxid.get()));
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
        public synchronized Optional<ListenableFuture<UnsignedLong>> apply(
                Optional<? extends SubmittedRequest<Records.Request, ?>> input)
                throws Exception {
            if (input.isPresent()) {
                final Operation.ProtocolResponse<?> response = input.get().get();
                if (Operations.maybeError(response.record(), KeeperException.Code.NONODE).isPresent()) {
                    if (((Records.PathGetter) input.get().request()).getPath().length() == fromRoot.length()) {
                        // root must exist
                        throw new KeeperException.NoNodeException(fromRoot.toString());
                    }
                } else {
                    if (input.get().request() instanceof IGetDataRequest) {
                        final long zxid = response.zxid();
                        final AbsoluteZNodePath fromPath = AbsoluteZNodePath.fromString(((Records.PathGetter) input.get().request()).getPath());
                        final Records.ZNodeStatGetter stat = new IStat(((Records.StatGetter) response).getStat());
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
                                .setData(((Records.DataGetter) response).getData());
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
                return Optional.<ListenableFuture<UnsignedLong>>of(this);
            }
        }
        
        protected <V,T extends ListenableFuture<V>> T updateZxid(long zxid, T future) {
            new ZxidUpdater<V>(zxid, future);
            return future;
        }
        
        @Override
        protected ListenableFuture<UnsignedLong> delegate() {
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
                addListener(this, SameThreadExecutor.getInstance());
            }

            @Override
            public void run() {
                if (isDone()) {
                    try {
                        get();
                        WalkerProcessor.this.zxid.update(this.zxid);
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    } catch (ExecutionException e) {
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
                    if (input.request() instanceof ICreateRequest) {
                        ICreateRequest request = (ICreateRequest) input.request();
                        Optional<Operation.Error> error = Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS);
                        if (CreateMode.valueOf(request.getFlags()) == CreateMode.PERSISTENT_SEQUENTIAL) {
                            if (!error.isPresent()) {
                                paths.add(ZNodePath.fromString(request.getPath()), ZNodePath.fromString(((ICreateResponse) response.record()).getPath()));
                            } else {
                                // TODO
                                throw new UnsupportedOperationException();
                            }
                        }
                    } else if (input.request() instanceof IDeleteRequest) {
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
        
        protected final class EphemeralZNodeLookups extends RunOnEmptyActor<StampedValue<ZNode>, StorageSchema.Safari.Sessions.Session.Data, ValueSessionLookup<StampedValue<ZNode>>> {

            protected final ZNodePath prefix;
            protected final Function<ZNodePath,ZNodeLabel> labelOf;
            
            public EphemeralZNodeLookups(
                    ZNodePath prefix,
                    Function<ZNodePath,ZNodeLabel> labelOf) {
                super(WalkerProcessor.this,
                        Queues.<ValueSessionLookup<StampedValue<ZNode>>>newConcurrentLinkedQueue(),
                        LogManager.getLogger(EphemeralZNodeLookups.class));
                this.prefix = prefix;
                this.labelOf = labelOf;
            }

            @Override
            public ListenableFuture<StorageSchema.Safari.Sessions.Session.Data> submit(StampedValue<ZNode> value) {
                ValueSessionLookup<StampedValue<ZNode>> lookup = ValueSessionLookup.create(
                        value, 
                        Long.valueOf(value.get().getStat().getEphemeralOwner()),
                        materializer);
                if (!send(lookup)) {
                    lookup.cancel(false);
                }
                return lookup;
            }
            
            @Override
            protected boolean doApply(ValueSessionLookup<StampedValue<ZNode>> input) throws Exception {
                StorageSchema.Safari.Sessions.Session.Data data;
                try { 
                    data = input.get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KeeperException.NoNodeException) {
                        return true;
                    } else {
                        throw e;
                    }
                }
                ZNodePath path = prefix.join(StorageSchema.Safari.Volumes.Volume.Snapshot.Ephemerals.Session.labelOf(data.getSessionId()));
                WalkerProcessor.this.submit(Pair.create(Optional.<Long>absent(), Operations.Requests.create().setPath(path).build()));
                path = path.join(
                        labelOf.apply(input.value().get().getCreate().getPath()));
                ByteBufOutputArchive buf = new ByteBufOutputArchive(Unpooled.buffer());
                Records.Requests.serialize(input.value().get().getCreate().build(), buf);
                WalkerProcessor.this.submit(Pair.create(
                        Optional.of(Long.valueOf(input.value().stamp())), 
                        Operations.Requests.create().setPath(path).setData(buf.get().array()).build()));
                return true;
            }
        }
        
        protected final class FromListener implements SessionListener, Runnable {
        
            protected final Set<Change<?>> changes;
            
            public FromListener() {
                this.changes = Sets.newHashSet();
                fromClient.subscribe(this);
                WalkerProcessor.this.addListener(this, SameThreadExecutor.getInstance());
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
                            WalkerProcessor.this.promise.setException(new KeeperException.NoNodeException(path.toString()));
                        }
                        WalkerProcessor.this.submit(
                                Pair.create(
                                        Optional.of(Long.valueOf(notification.zxid())),
                                        Operations.Requests.delete().setPath(
                                                paths.apply(path)).build()));
                        break;
                    }
                    default:
                        break;
                    }
                    if (change != null) {
                        changes.add(change.get());
                        change.get().addListener(change.get(), SameThreadExecutor.getInstance());
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
                        if (getFromChildren.requests().get(i).opcode() == OpCode.GET_CHILDREN) {
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
                        if (getData.requests().get(i).opcode() == OpCode.GET_DATA) {
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
    
    protected static final char ESCAPE_CHAR = '\\';
    protected static final String ESCAPE_ESCAPE = "\\\\";
    
    protected static final Function<String,String> ESCAPE_ZNODE_NAME = new Function<String,String>() {
        @Override
        public String apply(String input) {
            return input.replace(String.valueOf(ESCAPE_CHAR), ESCAPE_ESCAPE).replace(ZNodePath.SLASH, ESCAPE_CHAR);
        }
    };
    
    protected static final class ValueSessionLookup<V> extends ForwardingListenableFuture<StorageSchema.Safari.Sessions.Session.Data> {

        public static <V> ValueSessionLookup<V> create(
                V value,
                Long session,
                Materializer<StorageZNode<?>,?> materializer) {
            return new ValueSessionLookup<V>(
                    value,
                    SessionLookup.create(
                            session, 
                            materializer,
                            SettableFuturePromise.<StorageSchema.Safari.Sessions.Session.Data>create()));
        }
        
        private final V value;
        private final ListenableFuture<StorageSchema.Safari.Sessions.Session.Data> future;
        
        protected ValueSessionLookup(
                V value,
                ListenableFuture<StorageSchema.Safari.Sessions.Session.Data> future) {
            this.value = value;
            this.future = future;
        }
        
        public V value() {
            return value;
        }
        
        @Override
        protected ListenableFuture<StorageSchema.Safari.Sessions.Session.Data> delegate() {
            return future;
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
        protected ZNodePath join(ZNodeName remaining) {
            ValueNode<ZNodeName> node = trie.root();
            Iterator<ZNodeName> prefixes = SequenceIterator.forName(remaining);
            List<String> names = Lists.newLinkedList();
            while (prefixes.hasNext()) {
                ZNodeName next = prefixes.next();
                node = node.get(next);
                if (node != null) {
                    names.add(node.get().toString());
                } else {
                    checkArgument(!prefixes.hasNext(), String.valueOf(next));
                }
            }
            return super.join(ZNodeName.fromString(ZNodeLabelVector.join(names.iterator())));
        }
        
        public static final class SequenceIterator extends AbstractIterator<ZNodeName> {

            public static SequenceIterator forName(ZNodeName name) {
                final Iterator<ZNodeLabel> labels;
                if (name instanceof EmptyZNodeLabel) {
                    labels = Iterators.emptyIterator();
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
