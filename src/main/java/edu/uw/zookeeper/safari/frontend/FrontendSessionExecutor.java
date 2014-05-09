package edu.uw.zookeeper.safari.frontend;

import java.util.Collections;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Actors.ExecutedPeekingQueuedActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
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
import edu.uw.zookeeper.safari.backend.OutdatedVersionException;
import edu.uw.zookeeper.safari.common.LockingCachedFunction;
import edu.uw.zookeeper.safari.common.WaitingActor;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.data.NullVolume;
import edu.uw.zookeeper.safari.data.VersionedId;
import edu.uw.zookeeper.safari.data.VersionedVolume;
import edu.uw.zookeeper.safari.data.Volume;
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.ShardedClientRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedErrorResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;
import edu.uw.zookeeper.server.AbstractSessionExecutor;

public class FrontendSessionExecutor extends AbstractSessionExecutor implements Processors.UncheckedProcessor<Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>, Message.ServerResponse<?>> {
    
    public static Factory factory(
            boolean renew,
            Provider<? extends Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>>> processor,
            LockingCachedFunction<Identifier, VersionedVolume> idToVolume,
            AsyncFunction<ZNodePath, Volume> pathToVolume,
            Supplier<Set<Identifier>> regions,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            ScheduledExecutorService scheduler,
            Executor executor) {
        return new Factory(
                        renew,
                        processor,
                        idToVolume,
                        pathToVolume,
                        regions,
                        dispatchers,
                        scheduler,
                        executor);
    }
    
    public static class Factory implements ParameterizedFactory<Session, FrontendSessionExecutor> {

        private final boolean renew;
        private final Provider<? extends Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>>> processor;
        private final LockingCachedFunction<Identifier, VersionedVolume> idToVolume;
        private final AsyncFunction<ZNodePath, Volume> pathToVolume;
        private final Supplier<Set<Identifier>> regions;
        private final AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers;
        private final ScheduledExecutorService scheduler;
        private final Executor executor;
        
        public Factory(
                boolean renew,
                Provider<? extends Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>>> processor,
                LockingCachedFunction<Identifier, VersionedVolume> idToVolume,
                AsyncFunction<ZNodePath, Volume> pathToVolume,
                Supplier<Set<Identifier>> regions,
                AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
                ScheduledExecutorService scheduler,
                Executor executor) {
            this.renew = renew;
            this.processor = processor;
            this.idToVolume = idToVolume;
            this.pathToVolume = pathToVolume;
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
            LockingCachedFunction<Identifier, VersionedVolume> idToVolume,
            AsyncFunction<ZNodePath, Volume> pathToVolume,
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
        return new FrontendSessionExecutor(processor, idToVolume, pathToVolume, regions, dispatchers, executor, renew, session, state, listeners, scheduler);
    }
    
    public static <T extends Records.Request> T validate(Volume volume, T request) throws KeeperException {
        switch (request.opcode()) {
        case DELETE:
        {
            // can't delete a volume root
            ZNodePath path = ZNodePath.fromString(((Records.PathGetter) request).getPath());
            assert (Volume.contains(volume, path));
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
                if (! Volume.contains(volume, path)) {
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
    protected final ClientPeerConnectionExecutors backends;
    protected final ShardingProcessor sharding;
    protected final Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor;
    protected final FrontendSessionActor actor;
    protected final NotificationProcessor notifications;

    public FrontendSessionExecutor(
            Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor,
            LockingCachedFunction<Identifier, VersionedVolume> idToVolume,
            AsyncFunction<ZNodePath, Volume> pathToVolume,
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
        this.notifications = new NotificationProcessor();
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
            task = new FrontendRequestTask(
                    Pair.create(request,
                            LoggingPromise.create(
                                    logger, 
                                    SettableFuturePromise.<Records.Response>create())), 
                    LoggingPromise.create(
                            logger, 
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
    
    protected class FrontendRequestTask extends PromiseTask<Pair<? extends Message.ClientRequest<?>, ? extends Promise<Records.Response>>, Message.ServerResponse<?>> implements Runnable {

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

    protected class FrontendSessionActor extends ExecutedPeekingQueuedActor<FrontendRequestTask> {

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
                if (! input.isDone()) {
                    try {
                        Records.Response response = input.task().second().get();
                        Message.ServerResponse<?> result = 
                                FrontendSessionExecutor.this.apply( 
                                        Pair.create(
                                                Optional.<Operation.ProtocolRequest<?>>of(input.task().first()), 
                                                response));
                        input.set(result);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        input.setException(e.getCause());
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
    protected class NotificationProcessor implements NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> {

        protected final Set<String> dataWatches;
        protected final Set<String> childWatches;
        
        public NotificationProcessor() {
            this.dataWatches = Collections.synchronizedSet(
                    Collections.newSetFromMap(Maps.<String, Boolean>newHashMap()));
            this.childWatches = Collections.synchronizedSet(
                    Collections.newSetFromMap(Maps.<String, Boolean>newHashMap()));
        }
        
        public void handleRequest(Records.Request request) {
            switch (request.opcode()) {
            case GET_DATA:
            case EXISTS:
                if (((Records.WatchGetter) request).getWatch()) {
                    dataWatches.add(((Records.PathGetter) request).getPath());
                }
                break;
            case GET_CHILDREN:
            case GET_CHILDREN2:
                if (((Records.WatchGetter) request).getWatch()) {
                    childWatches.add(((Records.PathGetter) request).getPath());
                }
                break;
            default:
                break;
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void handleNotification(
                ShardedServerResponseMessage<IWatcherEvent> notification) {
            IWatcherEvent event = notification.record();
            Watcher.Event.EventType type = Watcher.Event.EventType.fromInt(event.getType());
            Set<String> watches = (type == Watcher.Event.EventType.NodeChildrenChanged) ? childWatches : dataWatches;
            if (watches.remove(event.getPath())) {
                // FIXME should we be filtering here or just pass everything on?
                FrontendSessionExecutor.this.handleNotification((Message.ServerResponse<IWatcherEvent>) apply(
                        Pair.create( 
                                Optional.<Operation.ProtocolRequest<?>>absent(),
                                (Records.Response) notification.record()))); 
            }
        }
    }
    
    protected static class ShardTask extends ForwardingListenableFuture<VersionedVolume> {

        private final ListenableFuture<? extends VersionedVolume> future;
        private final ListenableFuture<?> task;
        
        public ShardTask(
                ListenableFuture<?> task,
                ListenableFuture<? extends VersionedVolume> future) {
            this.task = task;
            this.future = future;
        }
        
        public ListenableFuture<?> task() {
            return task;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected ListenableFuture<VersionedVolume> delegate() {
            return (ListenableFuture<VersionedVolume>) future;
        }
    }
    
    protected static class RequestToVolume implements AsyncFunction<Records.Request, VersionedVolume> {

        protected final AsyncFunction<ZNodePath, Volume> pathToVolume;
        
        public RequestToVolume(AsyncFunction<ZNodePath, Volume> pathToVolume) {
            this.pathToVolume = pathToVolume;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public ListenableFuture<VersionedVolume> apply(Records.Request input) {
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
                return Futures.<VersionedVolume>immediateFuture(NullVolume.getInstance());
            } else {
                try {
                    // lame
                    return (ListenableFuture<VersionedVolume>) (ListenableFuture<?>) pathToVolume.apply(path);
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }
        }
    }
    
    protected class ShardingProcessor extends WaitingActor<ListenableFuture<?>, ShardTask> implements TaskExecutor<ListenableFuture<?>, VersionedVolume> {

        protected final LockingCachedFunction<Identifier, VersionedVolume> idToVolume;
        protected final RequestToVolume requestToVolume;
        
        public ShardingProcessor(
                LockingCachedFunction<Identifier, VersionedVolume> idToVolume,
                AsyncFunction<ZNodePath, Volume> pathToVolume,
                Executor executor) {
            super(executor, Queues.<ShardTask>newConcurrentLinkedQueue(), FrontendSessionExecutor.this.logger);
            this.idToVolume = idToVolume;
            this.requestToVolume = new RequestToVolume(pathToVolume);
        }
        
        @Override
        public ListenableFuture<VersionedVolume> submit(ListenableFuture<?> request) {
            ListenableFuture<? extends VersionedVolume> shard;
            if (request instanceof FrontendRequestTask) {
                shard = requestToVolume.apply(((FrontendRequestTask) request).task().first().record());
            } else {
                assert (request instanceof OperationResponseTask);
                VersionedId outdated = Futures.getUnchecked((OperationResponseTask) request).getShard();
                idToVolume.lock().lock();
                try {
                    VersionedVolume current = idToVolume.cached().apply(outdated.getIdentifier());
                    if ((current == null) || (current.getVersion().longValue() <= outdated.getVersion().longValue())) {
                        try {
                            shard = idToVolume.async().apply(outdated.getIdentifier());
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
            ShardTask task = new ShardTask(request, shard);
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
                VersionedVolume shard;
                try {
                    shard = input.get();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                    // TODO 
                    throw new UnsupportedOperationException(e.getCause());
                }
                if (shard instanceof Volume) {
                    Volume volume = (Volume) shard;
                    Identifier region = volume.getRegion();
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
                            ShardedClientRequestMessage<?> message = ShardedClientRequestMessage.valueOf(
                                    VersionedId.valueOf(volume.getDescriptor().getId(), volume.getVersion()), 
                                    frontend.task().first());
                            // we shouldn't see a notification until we see the request
                            // for setting a notification
                            // note that storing the notification now is optimistic
                            notifications.handleRequest(message.record());
                            ListenableFuture<ShardedResponseMessage<?>> response;
                                response = executor.submit(message);
                            new OperationResponseTask(frontend, response);
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
                        new BroadcastResponseTask((FrontendRequestTask) input.task()).run();
                        return true;
                    }
                }
            } else {
                assert (input.task() instanceof OperationResponseTask);
                submit(((OperationResponseTask) input.task()).task());
            }
            return false;
        }
        
        protected class BroadcastResponseTask extends PromiseTask<Pair<? extends ShardedClientRequestMessage<?>, FrontendRequestTask>, Records.Response> implements Runnable, Callable<Optional<Records.Response>> {

            protected final CallablePromiseTask<BroadcastResponseTask, Records.Response> delegate;
            protected final Map<Identifier, ListenableFuture<?>> responses;
            
            public BroadcastResponseTask(FrontendRequestTask task) {
                super(Pair.create(ShardedClientRequestMessage.valueOf(
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
        
        protected class OperationResponseTask extends ForwardingListenableFuture<ShardedResponseMessage<?>> implements Runnable {

            protected final FrontendRequestTask task;
            protected final ListenableFuture<ShardedResponseMessage<?>> future;

            public OperationResponseTask(
                    FrontendRequestTask task,
                    ListenableFuture<ShardedResponseMessage<?>> future) {
                this.task = task;
                this.future = future;
                addListener(this, SameThreadExecutor.getInstance());
            }
            
            public FrontendRequestTask task() {
                return task;
            }
            
            // Must not be called more than once
            @Override
            public void run() {
                if (isDone()) {
                    if (isCancelled()) {
                        task.task().second().cancel(true);
                    } else {
                        ShardedResponseMessage<?> response;
                        try { 
                            response = get();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            // TODO
                            throw new UnsupportedOperationException(e.getCause());
                        }
                        if (response instanceof ShardedServerResponseMessage<?>) {
                            task.task().second().set(((ShardedServerResponseMessage<?>) response).record());
                        } else {
                            assert (((ShardedErrorResponseMessage) response).getException() instanceof OutdatedVersionException);
                            sharding.submit(this);
                        }
                    }
                }
            }

            @Override
            protected ListenableFuture<ShardedResponseMessage<?>> delegate() {
                return future;
            }
        }
    }
}