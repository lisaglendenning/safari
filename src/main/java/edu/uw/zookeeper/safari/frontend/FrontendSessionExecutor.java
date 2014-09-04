package edu.uw.zookeeper.safari.frontend;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;

import edu.uw.zookeeper.WatchType;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Actors.ExecutedPeekingQueuedActor;
import edu.uw.zookeeper.common.LockingCachedFunction;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.common.WaitingActor;
import edu.uw.zookeeper.data.OptionalNode;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.ProtocolMessageAutomaton;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.backend.OutdatedVersionException;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedErrorResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.NullVolume;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;
import edu.uw.zookeeper.server.AbstractSessionExecutor;

public class FrontendSessionExecutor extends AbstractSessionExecutor implements Processors.UncheckedProcessor<Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>, Message.ServerResponse<?>> {
    
    public static Factory factory(
            boolean renew,
            Provider<? extends Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>>> processor,
            LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume,
            AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume,
            AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
            AsyncFunction<VersionedId, Long> versionToXomega,
            Supplier<Set<Identifier>> regions,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            ScheduledExecutorService scheduler,
            Executor executor) {
        return new Factory(
                        renew,
                        processor,
                        idToVolume,
                        pathToVolume,
                        versionToVolume,
                        versionToXomega,
                        regions,
                        dispatchers,
                        scheduler,
                        executor);
    }
    
    public static class Factory implements ParameterizedFactory<Session, FrontendSessionExecutor> {

        private final boolean renew;
        private final Provider<? extends Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>>> processor;
        private final LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume;
        private final AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume;
        private final AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume;
        private final AsyncFunction<VersionedId, Long> versionToXomega;
        private final Supplier<Set<Identifier>> regions;
        private final AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers;
        private final ScheduledExecutorService scheduler;
        private final Executor executor;
        
        public Factory(
                boolean renew,
                Provider<? extends Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>>> processor,
                LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume,
                AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume,
                AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
                AsyncFunction<VersionedId, Long> versionToXomega,
                Supplier<Set<Identifier>> regions,
                AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
                ScheduledExecutorService scheduler,
                Executor executor) {
            this.renew = renew;
            this.processor = processor;
            this.idToVolume = idToVolume;
            this.pathToVolume = pathToVolume;
            this.versionToVolume = versionToVolume;
            this.versionToXomega = versionToXomega;
            this.regions = regions;
            this.dispatchers = dispatchers;
            this.scheduler = scheduler;
            this.executor = executor;
        }
        
        public FrontendSessionExecutor get(Session value) {
            return newInstance(
                    renew,
                    value, 
                    processor.get(),
                    idToVolume,
                    pathToVolume,
                    versionToVolume,
                    versionToXomega,
                    regions,
                    dispatchers,
                    scheduler,
                    executor);
        }
    }
    
    public static FrontendSessionExecutor newInstance(
            boolean renew,
            Session session,
            Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor,
            LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume,
            AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume,
            AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
            AsyncFunction<VersionedId, Long> versionToXomega,
            Supplier<Set<Identifier>> regions,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            ScheduledExecutorService scheduler,
            Executor executor) {
        Automatons.SynchronizedEventfulAutomaton<ProtocolState, Object,?> state =
                Automatons.createSynchronizedEventful(
                        Automatons.createEventful(
                                Automatons.createLogging(
                                        LogManager.getLogger(FrontendSessionExecutor.class),
                                        ProtocolMessageAutomaton.asAutomaton(
                                                ProtocolState.CONNECTED))));
        IConcurrentSet<SessionListener> listeners = new StrongConcurrentSet<SessionListener>();
        return new FrontendSessionExecutor(processor, idToVolume, pathToVolume, versionToVolume, versionToXomega, regions, dispatchers, executor, renew, session, state, listeners, scheduler);
    }
    
    public static <T extends Records.Request> T validate(AssignedVolumeBranches volume, T request) throws KeeperException {
        switch (request.opcode()) {
        case DELETE:
        {
            // can't delete a volume root
            ZNodePath path = ZNodePath.fromString(((Records.PathGetter) request).getPath());
            assert (AssignedVolumeBranches.contains(volume, path));
            if (volume.getDescriptor().getPath().length() == path.length()) {
                throw new KeeperException.BadArgumentsException(path.toString());
            }
            break;
        }
        case MULTI:
        {
            // multi only implemented when all operations are on the same volume
            for (Records.MultiOpRequest op: (IMultiRequest) request) {
                ZNodePath path = ZNodePath.fromString(op.getPath());
                if (! AssignedVolumeBranches.contains(volume, path)) {
                    throw new KeeperException.BadArgumentsException(path.toString());
                }
                validate(volume, op);
            }
            break;
        }
        default:
            break;
        }
        return request;
    }

    protected final Logger logger;
    protected final Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor;
    protected final FrontendSessionActor actor;
    protected final NotificationProcessor notifications;
    protected final ClientPeerConnectionExecutors backends;
    protected final ShardingProcessor sharding;

    protected FrontendSessionExecutor(
            Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor,
            LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume,
            AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume,
            AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
            AsyncFunction<VersionedId, Long> versionToXomega,
            Supplier<Set<Identifier>> regions,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            Executor executor,
            boolean renew,
            Session session,
            Automatons.EventfulAutomaton<ProtocolState, Object> state,
            IConcurrentSet<SessionListener> listeners,
            ScheduledExecutorService scheduler) {
        super(session, state, listeners, scheduler);
        this.logger = LogManager.getLogger(getClass());
        this.processor = processor;
        this.notifications = new NotificationProcessor(versionToVolume, versionToXomega);
        this.backends = ClientPeerConnectionExecutors.newInstance(session, renew, notifications, regions, dispatchers);
        this.actor = new FrontendSessionActor(executor);
        this.sharding = new ShardingProcessor(idToVolume, pathToVolume, executor);
    }
    
    public ClientPeerConnectionExecutors backends() {
        return backends;
    }
    
    public NotificationProcessor notifications() {
        return notifications;
    }
    
    @Override
    protected ListenableFuture<Message.ServerResponse<?>> doSubmit(
            Message.ClientRequest<?> request) {
        final FrontendRequestTask task;
        switch (request.record().opcode()) {
        case PING:
        {
            // short circuit pings to keep connection alive
            return Futures.<Message.ServerResponse<?>>immediateFuture(
                    apply(Pair.create(Optional.<Operation.ProtocolRequest<?>>of(request), 
                                    (Records.Response) Records.newInstance(IPingResponse.class))));
        }
        case AUTH:
        // TODO
        case SET_WATCHES:
        {
            throw new RejectedExecutionException(new KeeperException.UnimplementedException());
        }
        default: 
        {
            task = LoggingFutureListener.listen(logger, 
                    new FrontendRequestTask(
                            Pair.create(request, 
                                    SettableFuturePromise.<Records.Response>create()), 
                            SettableFuturePromise.<Message.ServerResponse<?>>create()));
            break;
        }
        }
        if (! actor.send(task)) {
            task.cancel(true);
            throw new RejectedExecutionException();
        }
        return task;
    }

    @Override
    public Message.ServerResponse<?> apply(Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response> input) {
        return processor.apply(Pair.create(session.id(), input));
    }
    
    protected final class FrontendRequestTask extends PromiseTask<Pair<? extends Message.ClientRequest<?>, ? extends Promise<Records.Response>>, Message.ServerResponse<?>> implements Runnable {

        public FrontendRequestTask(
                Pair<? extends Message.ClientRequest<?>, ? extends Promise<Records.Response>> task,
                Promise<Message.ServerResponse<?>> promise) {
            super(task, promise);
            
            task.second().addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public void run() {
            if (task().second().isDone()) {
                if (this == actor.mailbox().peek()) {
                    actor.run();
                }
            }
        }
    }

    protected final class FrontendSessionActor extends ExecutedPeekingQueuedActor<FrontendRequestTask> {

        public FrontendSessionActor(
                Executor executor) {
            super(executor, new ConcurrentLinkedQueue<FrontendRequestTask>(), FrontendSessionExecutor.this.logger);
        }
        
        public Queue<FrontendRequestTask> mailbox() {
            return mailbox;
        }
        
        @Override
        public boolean isReady() {
            FrontendRequestTask next = mailbox.peek();
            return ((next != null) && (next.task().second().isDone()));
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(FrontendSessionExecutor.this).toString();
        }

        @Override
        protected synchronized boolean doSend(FrontendRequestTask message) {
            if (! mailbox.offer(message)) {
                return false;
            }
            try {
                sharding.submit(message);
                if (!schedule() && (state() == State.TERMINATED)) {
                    throw new RejectedExecutionException();
                }
            } catch (RejectedExecutionException e) {
                mailbox.remove(message);
                return false;
            }
            return true;
        }

        @Override
        protected synchronized boolean apply(FrontendRequestTask input) {
            if (!input.isDone() && !input.task().second().isDone()) {
                return false;
            }
            if (mailbox.remove(input)) {
                if (!input.isDone()) {
                    if (input.task().second().isCancelled()) {
                        input.cancel(false);
                    } else {
                        try {
                            Records.Response response = input.task().second().get();
                            Message.ServerResponse<?> result = 
                                    FrontendSessionExecutor.this.apply( 
                                            Pair.create(
                                                    Optional.<Operation.ProtocolRequest<?>>of(input.task().first()), 
                                                    response));
                            input.set(result);
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        } catch (ExecutionException e) {
                            input.setException(e.getCause());
                        }
                    }
                }
                return true;
            }
            return false;
        }
        
        @Override
        protected synchronized void doStop() {
            FrontendRequestTask next;
            while ((next = mailbox.poll()) != null) {
                next.cancel(true);
            }
            sharding.stop();
            backends.stop();
        }
    }
    
    /**
     * Tracks session watches
     */
    protected final class NotificationProcessor implements NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> {

        protected final AsyncFunction<VersionedId, Long> versionToXomega;
        protected final AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume;
        protected final SimpleLabelTrie<OptionalNode<Watch>> watches;
        protected final Map<Identifier, UnsignedLong> latest;
        
        public NotificationProcessor(
                AsyncFunction<VersionedId, AssignedVolumeBranches> versionToVolume,
                AsyncFunction<VersionedId, Long> versionToXomega) {
            this.versionToXomega = versionToXomega;
            this.versionToVolume = versionToVolume;
            this.watches = SimpleLabelTrie.forRoot(OptionalNode.<Watch>root());
            this.latest = Maps.newHashMap();
        }
        
        public synchronized Optional<? extends ListenableFuture<?>> handleRequest(Records.Request request, ShardedServerResponseMessage<?> response) throws Exception {
            final VersionedId shard = response.getShard();
            if (!latest.containsKey(shard.getValue()) || (latest.get(shard.getValue()).longValue() < shard.getVersion().longValue())) {
                final ListenableFuture<AssignedVolumeBranches> volumeFuture = versionToVolume.apply(shard);
                if (!volumeFuture.isDone()) {
                    return Optional.of(volumeFuture);
                }
                final AssignedVolumeBranches volume = volumeFuture.get();
                OptionalNode<Watch> node = watches.get(volume.getDescriptor().getPath());
                Queue<OptionalNode<Watch>> nodes = Queues.newArrayDeque();
                if (node != null) {
                    nodes.add(node);
                }
                while ((node = nodes.poll()) != null) {
                    for (OptionalNode<Watch> child: node.values()) {
                        if (!volume.getState().getValue().getBranches().containsKey(child.path().suffix(volume.getDescriptor().getPath()))) {
                            nodes.add(child);
                        }
                    }
                    if (node.isPresent() && (node.get().getVersion().getVersion().longValue() < shard.getVersion().longValue())) {
                        if (node.get().getType() != WatchType.CHILD) {
                            final ListenableFuture<Long> xomegaFuture = versionToXomega.apply(node.get().getVersion());
                            if (!xomegaFuture.isDone()) {
                                return Optional.of(xomegaFuture);
                            }
                            final long xomega = xomegaFuture.get().longValue();
                            if (node.get().getZxid() == xomega) {
                                handleNotification(new IWatcherEvent(Watcher.Event.EventType.NodeDataChanged.getIntValue(), Watcher.Event.KeeperState.SyncConnected.getIntValue(), node.path().toString()));
                            }
                        }
                        if (node.isPresent() && (node.get().getType() != WatchType.DATA)) {
                            handleNotification(new IWatcherEvent(Watcher.Event.EventType.NodeChildrenChanged.getIntValue(), Watcher.Event.KeeperState.SyncConnected.getIntValue(), node.path().toString()));
                        }
                    }
                }
                latest.put(shard.getValue(), shard.getVersion());
            }
            
            WatchType type = null;
            switch (request.opcode()) {
            case GET_DATA:
            case EXISTS:
                if (((Records.WatchGetter) request).getWatch()) {
                    type = WatchType.DATA;
                }
                break;
            case GET_CHILDREN:
            case GET_CHILDREN2:
                if (((Records.WatchGetter) request).getWatch()) {
                    type = WatchType.CHILD;
                }
                break;
            default:
                break;
            }
            if (type != null) {
                OptionalNode<Watch> node = OptionalNode.putIfAbsent(watches, ZNodePath.fromString(((Records.PathGetter) request).getPath()));
                if (node.isPresent()) {
                    if ((node.get().getType() != type) && (node.get().getType() != WatchType.CHILD_AND_DATA)) {
                        // keep the older versioning
                        node.set(new Watch(WatchType.CHILD_AND_DATA, node.get().getVersion(), node.get().getZxid()));
                    }
                } else {
                    node.set(new Watch(type, shard, response.zxid()));
                }
            }
            
            return Optional.absent();
        }
        
        @Override
        public synchronized void handleNotification(
                ShardedServerResponseMessage<IWatcherEvent> notification) {
            handleNotification(notification.record());
        }
        
        @SuppressWarnings("unchecked")
        public synchronized void handleNotification(IWatcherEvent event) {
            final Watcher.Event.EventType eventType = Watcher.Event.EventType.fromInt(event.getType());
            // TODO is None ever actually sent?
            boolean deliver = (eventType == Watcher.Event.EventType.None);
            if (!deliver) {
                OptionalNode<Watch> node = watches.get(event.getPath());
                if ((node != null) && node.isPresent()) {
                    final WatchType type = node.get().getType();
                    switch (eventType) {
                    case NodeChildrenChanged:
                    {
                        switch (type) {
                        case CHILD:
                            node.unset();
                            break;
                        case CHILD_AND_DATA:
                            node.set(new Watch(WatchType.DATA, node.get().getVersion(), node.get().getZxid()));
                            break;
                        default:
                            break;
                        }
                        break;
                    }
                    case NodeCreated:
                    case NodeDataChanged:
                    {
                        switch (type) {
                        case DATA:
                            node.unset();
                            break;
                        case CHILD_AND_DATA:
                            node.set(new Watch(WatchType.CHILD, node.get().getVersion(), node.get().getZxid()));
                            break;
                        default:
                            break;
                        }
                        break;
                    }
                    case NodeDeleted:
                    {
                        switch (type) {
                        case DATA:
                        case CHILD:
                            node.unset();
                            break;
                        case CHILD_AND_DATA:
                            // data watch is triggered first
                            node.set(new Watch(WatchType.CHILD, node.get().getVersion(), node.get().getZxid()));
                            break;
                        default:
                            break;
                        }
                        break;
                    }
                    default:
                        throw new AssertionError();
                    }
                    if (!node.isPresent() || (node.get().getType() != type)) {
                        deliver = true;
                    }
                    while ((node != watches.root()) && node.isEmpty() && !node.isPresent()) {
                        node.remove();
                        node = node.parent().get();
                    }
                }
            }
            if (deliver) {
                // FIXME should we be filtering?
                FrontendSessionExecutor.this.handleNotification((Message.ServerResponse<IWatcherEvent>) apply(
                        Pair.create( 
                                Optional.<Operation.ProtocolRequest<?>>absent(),
                                (Records.Response) event))); 
            }
        }
    }
    
    protected static final class Watch {
        
        private final WatchType type;
        private final VersionedId version;
        private final long zxid;
        
        public Watch(WatchType type, VersionedId version, long zxid) {
            super();
            this.type = type;
            this.version = version;
            this.zxid = zxid;
        }

        public WatchType getType() {
            return type;
        }

        public VersionedId getVersion() {
            return version;
        }

        public long getZxid() {
            return zxid;
        }
        
        // TODO equals()
    }
    
    protected static final class ShardTask extends ForwardingListenableFuture<VolumeVersion<?>> {

        private final ListenableFuture<? extends VolumeVersion<?>> future;
        private final ListenableFuture<?> task;
        
        public ShardTask(
                ListenableFuture<?> task,
                ListenableFuture<? extends VolumeVersion<?>> future) {
            this.task = task;
            this.future = future;
        }
        
        public ListenableFuture<?> task() {
            return task;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected ListenableFuture<VolumeVersion<?>> delegate() {
            return (ListenableFuture<VolumeVersion<?>>) future;
        }
    }
    
    protected static final class RequestToVolume implements AsyncFunction<Records.Request, VolumeVersion<?>> {

        private final AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume;
        
        public RequestToVolume(AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume) {
            this.pathToVolume = pathToVolume;
        }
        
        @SuppressWarnings({ "unchecked" })
        @Override
        public ListenableFuture<VolumeVersion<?>> apply(Records.Request input) {
            ZNodePath path = null;
            switch (input.opcode()) {
            case CREATE:
            case CREATE2:
            case DELETE:
            case CHECK:
            case EXISTS:
            case GET_ACL:
            case GET_CHILDREN:
            case GET_CHILDREN2:
            case GET_DATA:
            case SET_ACL:
            case SET_DATA:
            case SYNC:
            {
                path = ZNodePath.fromString(((Records.PathGetter) input).getPath());
                break;
            }
            case MULTI:
            {
                // Note: we assume that empty multi is legal
                // we're going to assume that all ops are on the same volume
                // so just pick the first path to lookup
                // (we will validate this request later)
                for (Records.MultiOpRequest e: (IMultiRequest) input) {
                    path = ZNodePath.fromString(e.getPath());
                    break;
                }
                break;
            }
            case CLOSE_SESSION:
                break;
            default:
                throw new UnsupportedOperationException(String.valueOf(input));
            }
            if (path == null) {
                return Futures.<VolumeVersion<?>>immediateFuture(NullVolume.getInstance());
            } else {
                try {
                    // lame
                    return (ListenableFuture<VolumeVersion<?>>) (ListenableFuture<?>) pathToVolume.apply(path);
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }
        }
    }
    
    protected final class ShardingProcessor extends WaitingActor<ListenableFuture<?>, ShardTask> implements TaskExecutor<ListenableFuture<?>, VolumeVersion<?>> {

        private final LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume;
        private final RequestToVolume requestToVolume;
        
        public ShardingProcessor(
                LockingCachedFunction<Identifier, VolumeVersion<?>> idToVolume,
                AsyncFunction<ZNodePath, AssignedVolumeBranches> pathToVolume,
                Executor executor) {
            super(executor, Queues.<ShardTask>newConcurrentLinkedQueue(), FrontendSessionExecutor.this.logger);
            this.idToVolume = idToVolume;
            this.requestToVolume = new RequestToVolume(pathToVolume);
        }
        
        @Override
        public ListenableFuture<VolumeVersion<?>> submit(ListenableFuture<?> request) {
            ListenableFuture<? extends VolumeVersion<?>> shard;
            if (request instanceof FrontendRequestTask) {
                shard = requestToVolume.apply(((FrontendRequestTask) request).task().first().record());
            } else {
                VersionedId outdated = Futures.getUnchecked((OperationResponseTask) request).getShard();
                idToVolume.lock().lock();
                try {
                    VolumeVersion<?> current = idToVolume.cached().apply(outdated.getValue());
                    if ((current == null) || (current.getState().getVersion().longValue() <= outdated.getVersion().longValue())) {
                        try {
                            shard = idToVolume.async().apply(outdated.getValue());
                        } catch (Exception e) {
                            shard = Futures.immediateFailedFuture(e);
                        }
                    } else {
                        shard = Futures.immediateFuture(NullVolume.getInstance());
                    }
                } finally {
                    idToVolume.lock().unlock();
                }
            }
            ShardTask task = LoggingFutureListener.listen(logger,
                    new ShardTask(request, shard));
            if (!send(task)) {
                throw new RejectedExecutionException();
            }
            return task;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(FrontendSessionExecutor.this.toString()).toString();
        }

        @Override
        protected synchronized boolean apply(ShardTask input) throws Exception {
            if (! input.isDone()) {
                wait(input);
                return false;
            }
            
            if (input.task() instanceof FrontendRequestTask) {
                VolumeVersion<?> shard;
                try {
                    shard = input.get();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                    // TODO 
                    throw new UnsupportedOperationException(e.getCause());
                }
                if (shard instanceof AssignedVolumeBranches) {
                    AssignedVolumeBranches volume = (AssignedVolumeBranches) shard;
                    Identifier region = volume.getState().getValue().getRegion();
                    ListenableFuture<ClientPeerConnectionExecutor> backend = backends.asLookup().apply(region);
                    if (! backend.isDone()) {
                        wait(backend);
                        return false;
                    }
                    ClientPeerConnectionExecutor executor;
                    try {
                        executor = backend.get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        // TODO 
                        throw new UnsupportedOperationException(e.getCause());
                    }
                    if (mailbox.remove(input)) {
                        FrontendRequestTask frontend = (FrontendRequestTask) input.task();
                        try {
                            validate(volume, frontend.task().first().record());
                        } catch (KeeperException e) {
                            frontend.task().second().set(new IErrorResponse(e.code()));
                        }
                        if (! frontend.task().second().isDone()) {
                            ShardedRequestMessage<?> message = ShardedRequestMessage.valueOf(
                                    VersionedId.valueOf(volume.getState().getVersion(), volume.getDescriptor().getId()), 
                                    frontend.task().first());
                            ListenableFuture<ShardedResponseMessage<?>> response = 
                                    executor.submit(message);
                            LoggingFutureListener.listen(logger, 
                                    new OperationResponseTask(frontend, response));
                        }
                        return true;
                    }
                } else {
                    assert (shard instanceof NullVolume);
                    FrontendRequestTask head = actor.mailbox().peek();
                    if (head == null) {
                        // TODO
                        throw new UnsupportedOperationException();
                    } else if (head != input.task()) {
                        wait(head);
                        return false;
                    }
                    if (mailbox.remove(input)) {
                        LoggingFutureListener.listen(logger,
                                new BroadcastResponseTask((FrontendRequestTask) input.task()))
                                .run();
                        return true;
                    }
                }
            } else {
                assert (input.task() instanceof OperationResponseTask);
                submit(((OperationResponseTask) input.task()).task());
            }
            return false;
        }
        
        protected class BroadcastResponseTask extends PromiseTask<Pair<? extends ShardedRequestMessage<?>, FrontendRequestTask>, Records.Response> implements Runnable, Callable<Optional<Records.Response>> {

            protected final CallablePromiseTask<BroadcastResponseTask, Records.Response> delegate;
            protected final Map<Identifier, ListenableFuture<?>> responses;
            
            public BroadcastResponseTask(FrontendRequestTask task) {
                super(Pair.create(ShardedRequestMessage.valueOf(
                        VersionedId.zero(), 
                        task.task().first()), task),
                    task.task().second());
                this.responses = Maps.newHashMapWithExpectedSize(backends.asCache().size());
                this.delegate = CallablePromiseTask.create(this, this);
            }

            // FIXME try again on failure
            @Override
            public synchronized Optional<Records.Response> call() throws Exception {
                Set<Identifier> difference = Sets.difference(backends.asCache().keySet(), responses.keySet()).immutableCopy();
                if (difference.isEmpty()) {
                    Map<Identifier, ListenableFuture<?>> updates = Maps.newHashMapWithExpectedSize(responses.size());
                    for (Map.Entry<Identifier, ListenableFuture<?>> entry: responses.entrySet()) {
                        if (entry.getValue().isDone()) {
                            Object result = entry.getValue().get();
                            if (result instanceof ClientPeerConnectionExecutor) {
                                updates.put(entry.getKey(), ((ClientPeerConnectionExecutor) result).submit(task().first()));
                            }
                        }
                    }
                    if (updates.isEmpty()) {
                        for (Map.Entry<Identifier, ListenableFuture<?>> entry: responses.entrySet()) {
                            if (entry.getValue().isDone()) {
                                Object result = entry.getValue().get();
                                if (! (result instanceof ShardedResponseMessage<?>)) {
                                    return Optional.absent();
                                }
                            } else {
                                return Optional.absent();
                            }
                        }
                        // if we've received response messages from everyone, then we're done
                        return Optional.of(Records.Responses.getInstance().get(task.first().getRequest().record().opcode()));
                    } else {
                        responses.putAll(updates);
                        for (ListenableFuture<?> future: updates.values()) {
                            future.addListener(this, SameThreadExecutor.getInstance());
                        }
                    }
                } else {
                    Map<Identifier, ListenableFuture<?>> updates = Maps.newHashMapWithExpectedSize(difference.size());
                    for (Identifier region: difference) {
                        updates.put(region, backends.asLookup().apply(region));
                    }
                    responses.putAll(updates);
                    for (ListenableFuture<?> future: updates.values()) {
                        future.addListener(this, SameThreadExecutor.getInstance());
                    }
                }
                return Optional.absent();
            }

            @Override
            public synchronized void run() {
                delegate.run();
            }
        }
        
        protected class OperationResponseTask extends SimpleToStringListenableFuture<ShardedResponseMessage<?>> implements Runnable, Callable<Optional<Records.Response>> {

            protected final FrontendRequestTask task;
            protected final CallablePromiseTask<OperationResponseTask,Records.Response> delegate;

            public OperationResponseTask(
                    FrontendRequestTask task,
                    ListenableFuture<ShardedResponseMessage<?>> future) {
                super(future);
                this.task = task;
                this.delegate = CallablePromiseTask.create(this, task.task().second());
                delegate.addListener(this, SameThreadExecutor.getInstance());
                addListener(this, SameThreadExecutor.getInstance());
            }
            
            public FrontendRequestTask task() {
                return task;
            }
            
            @Override
            public void run() {
                if (delegate.isDone()) {
                    if (delegate.isCancelled()) {
                        cancel(false);
                    }
                } else {
                    delegate.run();
                }
            }

            @Override
            public Optional<Records.Response> call() throws Exception {
                if (isDone()) {
                    ShardedResponseMessage<?> result = get();
                    if (result instanceof ShardedServerResponseMessage<?>) {
                        ShardedServerResponseMessage<?> response = (ShardedServerResponseMessage<?>) result;
                        Optional<? extends ListenableFuture<?>> future = notifications.handleRequest(task.task().first().record(), response);
                        if (future.isPresent()) {
                            new Callback(future.get());
                        } else {
                            return Optional.of((Records.Response) response.record());
                        }
                    } else {
                        assert (((ShardedErrorResponseMessage) result).getError() instanceof OutdatedVersionException);
                        sharding.submit(this);
                    }
                }
                return Optional.absent();
            }
            
            @Override
            protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper helper) {
                return super.toStringHelper(helper.addValue(task)).addValue(toString(delegate));
            }
            
            protected class Callback implements Runnable {
                private final ListenableFuture<?> future;
                
                public Callback(ListenableFuture<?> future) {
                    this.future = future;
                    future.addListener(this, SameThreadExecutor.getInstance());
                }
                
                @Override
                public void run() {
                    if (future.isDone()) {
                        try {
                            future.get();
                        } catch (Exception e) {
                            delegate.setException(e);
                        }
                        OperationResponseTask.this.run();
                    }
                }
            }
        }
    }
}
