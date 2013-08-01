package edu.uw.zookeeper.orchestra.frontend;

import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Ping;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.PingProcessor;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processors;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TaskExecutor;

public class FrontendSessionExecutor extends AbstractActor<FrontendSessionExecutor.RequestFuture> implements TaskExecutor<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> {
    
    protected final Logger logger;
    protected final Session session;
    protected final Publisher publisher;
    protected final Lookups<ZNodeLabel.Path, Volume> volumes;
    protected final Lookups<Identifier, Identifier> assignments;
    protected final BackendLookups backends;
    protected final Processors.UncheckedProcessor<Pair<SessionOperation.Request<Records.Request>, Records.Response>, Message.ServerResponse<Records.Response>> processor;

    protected FrontendSessionExecutor(
            Session session,
            Publisher publisher,
            Processors.UncheckedProcessor<Pair<SessionOperation.Request<Records.Request>, Records.Response>, Message.ServerResponse<Records.Response>> processor,
            CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
            CachedFunction<Identifier, Identifier> assignmentLookup,
            Function<Identifier, Identifier> ensembleForPeer,
            CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup,
            Executor executor) {
        super(executor, FutureQueue.<RequestFuture>create(), AbstractActor.newState());
        this.logger = LoggerFactory.getLogger(getClass());
        this.session = session;
        this.publisher = publisher;
        this.processor = processor;
        this.volumes = new Lookups<ZNodeLabel.Path, Volume>(
                volumeLookup, 
                this,
                MoreExecutors.sameThreadExecutor());
        this.assignments = new Lookups<Identifier, Identifier>(
                assignmentLookup, 
                this,
                MoreExecutors.sameThreadExecutor());
        this.backends = new BackendLookups(ensembleForPeer, connectionLookup);
    }
    
    public Session session() {
        return session;
    }
    
    protected Lookups<ZNodeLabel.Path, Volume> volumes() {
        return volumes;
    }
    
    protected Lookups<Identifier, Identifier> assignments() {
        return assignments;
    }
    
    protected BackendLookups backends() {
        return backends;
    }
    
    protected Processors.UncheckedProcessor<Pair<SessionOperation.Request<Records.Request>, Records.Response>, Message.ServerResponse<Records.Response>> processor() {
        return processor;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
            Message.ClientRequest<Records.Request> request) {
        RequestFuture task = newRequest(request);
        send(task);
        return task;
    }
    
    @Override
    public void send(RequestFuture message) {
        // short circuit pings
        if (message.task().getXid() == OpCodeXid.PING.getXid()) {
            try {
                while (message.call() != RequestState.PUBLISHED) {}
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Submitting {}", message.task());
            }
            super.send(message);
            message.addListener(this, MoreExecutors.sameThreadExecutor());
        }
    }

    public RequestFuture newRequest(Message.ClientRequest<Records.Request> request) {
        Promise<Message.ServerResponse<Records.Response>> promise = SettableFuturePromise.create();
        if (request.getXid() == OpCodeXid.PING.getXid()) {
            return new PingTask(request, promise);
        } else {
            return new BackendRequestTask(request, promise);
        }
    }
    
    public void handleTransition(Identifier peer, Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            // FIXME
            throw new UnsupportedOperationException();
        }
    }
    
    public void handleResponse(Identifier peer, ShardedResponseMessage<?> message) {
        if (state.get() == State.TERMINATED) {
            // FIXME
            throw new IllegalStateException();
        }
        Identifier ensemble = backends().getEnsembleForPeer().apply(peer);
        backends().get().get(ensemble).handleResponse(message);
        schedule();
    }

    @Override
    protected boolean runEnter() {
        if (state.get() == State.WAITING) {
            schedule();
            return false;
        } else {
            return super.runEnter();
        }
    }

    @Override
    protected void doRun() throws Exception {
        RequestFuture prev = null;
        Iterator<RequestFuture> itr = mailbox.iterator();
        while (itr.hasNext()) {
            RequestFuture task = itr.next();
            if (! apply(task)) {
                // TODO
                break;
            }
            while (true) {
                RequestState state = task.state();
                if (RequestState.PUBLISHED == state) {
                    itr.remove();
                    break;
                }
                else if ((RequestState.LOOKING == state)
                        && ((prev != null) && (prev.state().compareTo(RequestState.SUBMITTING) < 0))) {
                    // we need to preserve ordering of requests per volume
                    // as a proxy for this requirement, 
                    // don't submit until the task before us has submitted
                    // (note this is stronger than necessary)
                    break;
                } else if (task.call() == state) {
                    break;
                }
            }
            
            prev = task;
        }
    }

    protected static class ListenableFuturePending<V> extends Pair<ListenableFuture<V>, Set<BackendRequestTask>> {
    
        public ListenableFuturePending(
                ListenableFuture<V> first) {
            super(first, Collections.synchronizedSet(Sets.<BackendRequestTask>newHashSet()));
        }
        
    }

    protected static class Lookups<I,O> extends Pair<CachedFunction<I,O>, ConcurrentMap<I, ListenableFuturePending<O>>> {
    
        protected final Runnable runnable;
        protected final Executor executor;
        
        public Lookups(
                CachedFunction<I, O> first,
                Runnable runnable,
                Executor executor) {
            super(first, new MapMaker().<I, ListenableFuturePending<O>>makeMap());
            this.runnable = runnable;
            this.executor = executor;
        }
        
        public Optional<O> apply(I input, BackendRequestTask listener) throws Exception {
            O output = first().first().apply(input);
            if (output != null) {
                return Optional.of(output);
            } else {
                ListenableFuturePending<O> pending = second().get(input);
                if (pending == null) {
                    ListenableFuture<O> future = first().apply(input);
                    pending = new ListenableFuturePending<O>(future);
                    ListenableFuturePending<O> prev = second().putIfAbsent(input, pending);
                    if (prev != null) {
                        future.cancel(true);
                        pending = prev;
                    } else {
                        pending.first().addListener(runnable, executor);
                    }
                }
                listener.pending.add(pending);
                pending.second().add(listener);
                // TODO: if pending was processed...
                return Optional.absent();
            }
        }
    }

    // TODO handle disconnects and reconnects
    protected class BackendLookups extends Lookups<Identifier, BackendSession> implements Reference<ConcurrentMap<Identifier, BackendSession>> {
    
        protected final ConcurrentMap<Identifier, BackendSession> backendSessions;
        protected final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup;
        protected final Function<Identifier, Identifier> ensembleForPeer;
        
        public BackendLookups(
                Function<Identifier, Identifier> ensembleForPeer,
                CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup) {
            this(ensembleForPeer, connectionLookup, 
                    new MapMaker().<Identifier, BackendSession>makeMap());
        }
        
        public BackendLookups(
                final Function<Identifier, Identifier> ensembleForPeer,
                final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup,
                final ConcurrentMap<Identifier, BackendSession> backendSessions) {
            super(CachedFunction.create(
                    new Function<Identifier, BackendSession>() {
                        @Override
                        public BackendSession apply(Identifier ensemble) {
                            return backendSessions.get(ensemble);
                        }
                    }, 
                    new AsyncFunction<Identifier, BackendSession>() {
                        @Override
                        public ListenableFuture<BackendSession> apply(Identifier ensemble) throws Exception {
                            final BackendSessionTask task = new BackendSessionTask(
                                    Optional.<Session>absent(),
                                    ensemble, 
                                    connectionLookup.apply(ensemble),
                                    SettableFuturePromise.<Session>create());
                            return Futures.transform(
                                    task, 
                                    new Function<Session, BackendSession>() {
                                        @Override
                                        @Nullable
                                        public BackendSession apply(Session input) {
                                            BackendSession backend = task.toBackendSession();
                                            BackendSession prev = backendSessions.putIfAbsent(backend.getEnsemble(), backend);
                                            if (prev != null) {
                                                // TODO
                                                throw new AssertionError();
                                            }
                                            return backend;
                                        }
                                        
                                    });
                        }
                    }),
                    FrontendSessionExecutor.this,
                    MoreExecutors.sameThreadExecutor());
            this.ensembleForPeer = ensembleForPeer;
            this.connectionLookup = connectionLookup;
            this.backendSessions = backendSessions;
        }
        
        public Function<Identifier, Identifier> getEnsembleForPeer() {
            return ensembleForPeer;
        }
        
        @Override
        public ConcurrentMap<Identifier, BackendSession> get() {
            return backendSessions;
        }
    }

    protected class BackendSessionTask extends PromiseTask<Identifier, Session> implements Runnable, FutureCallback<MessagePacket> {
        
        protected final Optional<Session> existing;
        protected final ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connection;
        
        public BackendSessionTask(
                Optional<Session> existing,
                Identifier ensemble,
                ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connection,
                Promise<Session> promise) throws Exception {
            super(ensemble, promise);
            this.existing = existing;
            this.connection = connection;
            this.connection.addListener(this, MoreExecutors.sameThreadExecutor());
        }
        
        public BackendSession toBackendSession() {
            return new BackendSession(
                    task(),
                    Futures.getUnchecked(this),
                    Futures.getUnchecked(connection()));
        }
        
        public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connection() {
            return connection;
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (event.to() == Connection.State.CONNECTION_CLOSED) {
                Futures.getUnchecked(this.connection).unregister(this);
                if (! isDone()) {
                    setException(new ClosedChannelException());
                }
            }
        }
        
        @Subscribe
        public void handleMessage(MessagePacket message) {
            switch (message.first().type()) {
            case MESSAGE_TYPE_SESSION_OPEN_RESPONSE:
            {
                MessageSessionOpenResponse response = message.getBody(MessageSessionOpenResponse.class);
                if (response.getSessionId() != session().id()) {
                    break;
                }
                Futures.getUnchecked(this.connection).unregister(this);
                if (! isDone()) {
                    if (response.delegate() instanceof ConnectMessage.Response.Valid) {
                        set(response.delegate().toSession());
                    } else {
                        setException(new KeeperException.SessionExpiredException());
                    }
                }
                break;
            }
            default:
                break;
            }
        }
        
        @Override
        public void run() {
            if (connection.isDone()) {
                ClientPeerConnection<?> c;
                try {
                    c = connection.get();
                } catch (Exception e) {
                    if (! isDone()) {
                        setException(e);
                    }
                    return;
                }
                
                ConnectMessage.Request request;
                if (existing.isPresent()) {
                    request = ConnectMessage.Request.RenewRequest.newInstance(
                                existing.get(), 0L);
                } else {
                    request = ConnectMessage.Request.NewRequest.newInstance(
                            session().parameters().timeOut(), 0L);
                }
                
                c.register(this);
                Futures.addCallback(
                        c.write(MessagePacket.of(
                                    MessageSessionOpenRequest.of(
                                        session().id(), request))),
                        this);
            }
        }

        @Override
        public void onSuccess(MessagePacket result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
    
    protected class BackendSession {

        private final Identifier ensemble;
        private final Session session;
        private final ClientPeerConnection<?> connection;
        // not thread safe
        private final LinkedList<ListenableFuture<ShardedResponseMessage<?>>> pending;
        private volatile int finger;

        public BackendSession(
                Identifier ensemble,
                Session session,
                ClientPeerConnection<?> connection) {
            this.ensemble = ensemble;
            this.session = session;
            this.connection = connection;
            this.pending = Lists.newLinkedList();
            this.finger = 0;
        }
        
        public Identifier getEnsemble() {
            return ensemble;
        }
        
        public Session getSession() {
            return session;
        }
        
        public ClientPeerConnection<?> getConnection() {
            return connection;
        }
        
        public synchronized BackendRequestTask.ResponseTask submit(
                BackendRequestTask task, ShardedRequestMessage<?> request) {
            MessagePacket message = MessagePacket.of(
                    MessageSessionRequest.of(session().id(), request));
            ListenableFuture<MessagePacket> future = getConnection().write(message);
            BackendRequestTask.ResponseTask result = task.new ResponseTask(request, future);
            pending.add(result);
            return result;
        }
        
        @SuppressWarnings("unchecked")
        @Subscribe
        public synchronized void handleResponse(ShardedResponseMessage<?> response) {
            if (OpCodeXid.NOTIFICATION.getXid() == response.getXid()) {
                ListenableFuture<? extends ShardedResponseMessage<?>> f = Futures.immediateFuture(response);
                pending.add(finger, (ListenableFuture<ShardedResponseMessage<?>>) f);
                finger += 1;
            } else {
                while (finger < pending.size()) {
                    BackendRequestTask.ResponseTask task = (BackendRequestTask.ResponseTask) pending.get(finger);
                    if (response.getXid() == task.task().getXid()) {
                        finger += 1;
                        if (! task.set(response)) {
                            logger.debug("Ignoring response {}", response);
                        }
                        break;
                    } else if (task.isDone()) {
                        finger += 1;
                    } else {
                        logger.debug("No match for response {}", response);
                        break;
                    }
                }
            }
        }
        
        public synchronized ListenableFuture<ShardedResponseMessage<?>> peek() {
            return pending.peek();
        }
        
        public synchronized ListenableFuture<ShardedResponseMessage<?>> poll() {
            ListenableFuture<ShardedResponseMessage<?>> next = pending.poll();
            if (next != null) {
                assert (finger > 0);
                finger -= 1;
            }
            return next;
        }
    }
    
    protected static enum RequestState {
        NEW, LOOKING, SUBMITTING, COMPLETE, PUBLISHED;
    }
    
    protected interface RequestFuture extends ListenableFuture<Message.ServerResponse<Records.Response>>, Stateful<FrontendSessionExecutor.RequestState>, Runnable, Callable<RequestState> {
        Message.ClientRequest<Records.Request> task();
    }
    
    protected abstract class RequestTask extends PromiseTask<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> implements RequestFuture {

        protected final AtomicReference<FrontendSessionExecutor.RequestState> state;

        public RequestTask(
                Message.ClientRequest<Records.Request> task) {
            this(task, SettableFuturePromise.<Message.ServerResponse<Records.Response>>create());
        }
        
        public RequestTask(
                Message.ClientRequest<Records.Request> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
            this.state = new AtomicReference<FrontendSessionExecutor.RequestState>(RequestState.NEW);
        }

        @Override
        public boolean set(Message.ServerResponse<Records.Response> result) {
            if ((result.getRecord().getOpcode() != task().getRecord().getOpcode())
                    && (! (result.getRecord() instanceof Operation.Error))) {
                throw new IllegalArgumentException(result.toString());
            }
            
            RequestState state = state();
            if (RequestState.COMPLETE.compareTo(state) > 0) {
                this.state.compareAndSet(state, RequestState.COMPLETE);
            }
            
            boolean isSet = super.set(result);
            if (logger.isTraceEnabled()) {
                logger.trace("Set {} -> {}", this, result);
            }
            return isSet;
        }
        
        @Override
        public FrontendSessionExecutor.RequestState state() {
            return state.get();
        }

        @Override
        public void run() {
            FrontendSessionExecutor.this.run();
        }

        @Override
        public RequestState call() throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("{}", this);
            }
            return state();
        }
        
        protected ListenableFuture<Message.ServerResponse<Records.Response>> complete() throws InterruptedException, ExecutionException {
            if (state.get() == RequestState.SUBMITTING) {
                Records.Response result = null;
                switch (task().getRecord().getOpcode()) {
                case CLOSE_SESSION:
                {
                    result = Records.newInstance(IDisconnectResponse.class);
                    break;
                }
                case PING:
                {
                    result = PingProcessor.getInstance().apply((Ping.Request) task().getRecord());
                    break;
                }
                default:
                    break;
                }
                if (result != null) {
                    complete(result);
                }
            }
            return this;
        }
        
        protected boolean complete(Records.Response result) {
            if (isDone()) {
                return false;
            }
            SessionOperation.Request<Records.Request> request =
                    SessionRequest.of(session().id(), task(), task());
            Message.ServerResponse<Records.Response> message = processor().apply(
                    Pair.create(request, result));
            return set(message);
        }

        protected boolean publish() {
            if (this.state.compareAndSet(RequestState.COMPLETE, RequestState.PUBLISHED)) {
                // TODO: exception?
                publisher.post(Futures.getUnchecked(this));
                return true;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("task", task())
                    .add("future", delegate())
                    .add("state", state())
                    .toString();
        }
    }
    
    protected class PingTask extends RequestTask {

        public PingTask(
                Message.ClientRequest<Records.Request> task) {
            this(task, SettableFuturePromise.<Message.ServerResponse<Records.Response>>create());
        }

        public PingTask(
                Message.ClientRequest<Records.Request> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
        }
        
        @Override
        public RequestState call() throws Exception {
            RequestState state = super.call();
            switch (state) {
            case NEW:
            case LOOKING:
                this.state.compareAndSet(state, RequestState.SUBMITTING);
            case SUBMITTING:
                complete();
                break;
            case COMPLETE:
                publish();
                break;
            default:
                break;
            }
            
            return state();
        }
    }

    protected class BackendRequestTask extends RequestTask {

        protected final ImmutableSet<ZNodeLabel.Path> paths;
        protected final Map<ZNodeLabel.Path, Volume> volumes;
        protected final BiMap<Volume, ShardedRequestMessage<?>> shards;
        protected final Map<Identifier, ResponseTask> submitted;
        protected final Set<ListenableFuturePending<?>> pending;

        public BackendRequestTask(
                Message.ClientRequest<Records.Request> task) {
            this(task, SettableFuturePromise.<Message.ServerResponse<Records.Response>>create());
        }
        
        public BackendRequestTask(
                Message.ClientRequest<Records.Request> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
            this.paths = ImmutableSet.copyOf(PathsOfRequest.getPathsOfRequest(task.getRecord()));
            this.volumes = Collections.synchronizedMap(Maps.<ZNodeLabel.Path, Volume>newHashMap());
            this.shards = Maps.synchronizedBiMap(HashBiMap.<Volume, ShardedRequestMessage<?>>create());
            this.submitted = Collections.synchronizedMap(Maps.<Identifier, ResponseTask>newHashMap());
            this.pending = Collections.synchronizedSet(Sets.<ListenableFuturePending<?>>newHashSet());
        }
        
        public ImmutableSet<ZNodeLabel.Path> paths() {
            return paths;
        }

        @Override
        public RequestState call() throws Exception {
            switch (super.call()) {
            case NEW:
            {
                Optional<Map<ZNodeLabel.Path, Volume>> volumes = getVolumes(paths());
                if (! volumes.isPresent()) {
                    break;
                }
                Set<Volume> uniqueVolumes = getUniqueVolumes(volumes.get().values());
                getConnections(uniqueVolumes);
                break;
            }
            case LOOKING:
            {
                Optional<Map<ZNodeLabel.Path, Volume>> volumes = getVolumes(paths());
                if (! volumes.isPresent()) {
                    break;
                }
                Set<Volume> uniqueVolumes = getUniqueVolumes(volumes.get().values());
                Optional<Map<Volume, Set<BackendSession>>> backends = getConnections(uniqueVolumes);
                if (! backends.isPresent()) {
                    break;
                }
                BiMap<Volume, ShardedRequestMessage<?>> shards;
                try {
                    shards = getShards(volumes.get());
                } catch (KeeperException e) {
                    // fail
                    complete(new IErrorResponse(e.code()));
                    break;
                }
                submit(shards, backends.get());
                break;
            }
            case SUBMITTING:
            {
                complete();
                break;
            }
            case COMPLETE:
            {
                publish();
                break;
            }
            default:
                break;
            }
            
            return state();
        }

        protected Optional<Map<ZNodeLabel.Path, Volume>> getVolumes(Set<ZNodeLabel.Path> paths) throws Exception {
            this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
            Sets.SetView<ZNodeLabel.Path> difference = Sets.difference(paths, volumes.keySet());
            for (ZNodeLabel.Path path: difference) {
                Optional<Volume> v = volumes().apply(path, this);
                if (v.isPresent()) {
                    volumes.put(path, v.get());
                }
            }
            if (difference.isEmpty()) {
                return Optional.of(volumes);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("Waiting for path lookup {}", difference);
                }
                return Optional.<Map<ZNodeLabel.Path, Volume>>absent();
            }
        }
        
        protected ImmutableSet<Volume> getUniqueVolumes(Collection<Volume> volumes) {
            return volumes.isEmpty()
                        ? ImmutableSet.of(Volume.none())
                        : ImmutableSet.copyOf(volumes);
        }
        
        protected BiMap<Volume, ShardedRequestMessage<?>> getShards(Map<ZNodeLabel.Path, Volume> volumes) throws KeeperException {
            this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
            ImmutableSet<Volume> uniqueVolumes = getUniqueVolumes(volumes.values());
            Sets.SetView<Volume> difference = Sets.difference(uniqueVolumes, shards.keySet());
            if (! difference.isEmpty()) {
                if ((OpCode.MULTI == task().getRecord().getOpcode())
                        && ! ImmutableSet.of(Volume.none()).equals(uniqueVolumes)) {
                    Map<Volume, List<Records.MultiOpRequest>> byShardOps = Maps.newHashMapWithExpectedSize(difference.size());
                    for (Records.MultiOpRequest op: (IMultiRequest) task().getRecord()) {
                        ZNodeLabel.Path[] paths = PathsOfRequest.getPathsOfRequest(op);
                        assert (paths.length > 0);
                        for (ZNodeLabel.Path path: paths) {
                            Volume v = volumes.get(path);
                            if (! difference.contains(v)) {
                                continue;
                            }
                            List<Records.MultiOpRequest> ops;
                            if (byShardOps.containsKey(v)) {
                                ops = byShardOps.get(v);
                            } else {
                                ops = Lists.newLinkedList();
                                byShardOps.put(v, ops);
                            }
                            ops.add(validate(v, op));
                        }
                    }
                    for (Map.Entry<Volume, List<Records.MultiOpRequest>> e: byShardOps.entrySet()) {
                        shards.put(e.getKey(), 
                                ShardedRequestMessage.of(
                                        e.getKey().getId(),
                                        ProtocolRequestMessage.of(
                                                task().getXid(),
                                                new IMultiRequest(e.getValue()))));
                    }
                } else {
                    for (Volume v: difference) {
                        validate(v, task().getRecord());
                        shards.put(v, ShardedRequestMessage.of(v.getId(), task()));
                    }
                }
            }
            assert (difference.isEmpty());
            return shards;
        }
        
        protected <T extends Records.Request> T validate(Volume volume, T request) throws KeeperException {
            switch (request.getOpcode()) {
            case CREATE:
            case CREATE2:
            {
                // special case: root of a volume can't be sequential!
                Records.CreateModeGetter create = (Records.CreateModeGetter) request;
                if (CreateMode.fromFlag(create.getFlags()).isSequential()
                        && volume.getDescriptor().getRoot().toString().equals(create.getPath())) {
                    // fail
                    throw new KeeperException.BadArgumentsException(create.getPath());
                }
            }
            default:
                break;
            }
            return request;
        }

        protected Optional<Map<Volume, Set<BackendSession>>> getConnections(Set<Volume> volumes) throws Exception {
            this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
            Map<Volume, Set<BackendSession>> backends = Maps.newHashMapWithExpectedSize(volumes.size());
            for (Volume v: volumes) {
                if (v.equals(Volume.none())) {
                    boolean done = true;
                    Set<BackendSession> all = Sets.newHashSetWithExpectedSize(backends().get().size());
                    for (Identifier ensemble: backends().get().keySet()) {
                        Optional<BackendSession> backend = backends().apply(ensemble, this);
                        if (backend.isPresent()) {
                            all.add(backend.get());
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Waiting for backend {}", ensemble);
                            }
                            done = false;
                        }
                    }
                    if (done) {
                        backends.put(v, all);
                    }
                } else {
                    Optional<Identifier> assignment = assignments().apply(v.getId(), this);
                    if (assignment.isPresent()) {
                        Optional<BackendSession> backend = backends().apply(assignment.get(), this);
                        if (backend.isPresent()) {
                            backends.put(v, ImmutableSet.of(backend.get()));
                        }
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Waiting for assignment {}", v.getId());
                        }
                    }
                }
            }
            Sets.SetView<Volume> difference = Sets.difference(volumes, backends.keySet());
            if (difference.isEmpty()) {
                return Optional.of(backends);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("Waiting for volume lookup {}", difference);
                }
                return Optional.absent();
            }
        }
        
        protected Map<Identifier, ResponseTask> submit(
                Map<Volume, ShardedRequestMessage<?>> shards,
                Map<Volume, Set<BackendSession>> backends) throws Exception {
            this.state.compareAndSet(RequestState.LOOKING, RequestState.SUBMITTING);
            for (Map.Entry<Volume, ShardedRequestMessage<?>> e: shards.entrySet()) {
                for (BackendSession backend: backends.get(e.getKey())) {
                    ClientPeerConnection<?> connection = backend.getConnection();
                    Identifier k = connection.remoteAddress().getIdentifier();
                    if (! submitted.containsKey(k)) {
                        ResponseTask task = backend.submit(this, e.getValue());
                        submitted.put(k, task);
                    }
                }
            }
            return submitted;
        }
        
        @Override
        protected ListenableFuture<Message.ServerResponse<Records.Response>> complete() throws InterruptedException, ExecutionException {
            if (state.get() != RequestState.SUBMITTING) {
                return this;
            }
            
            if (submitted.isEmpty()) {
                return super.complete();
            }
            
            Records.Response result = null;
            if (OpCode.MULTI == task().getRecord().getOpcode()) {
                Map<Volume, ListIterator<Records.MultiOpResponse>> responses = Maps.newHashMapWithExpectedSize(shards.size());
                for (ResponseTask task: submitted.values()) {
                    if (! task.isDone()) {
                        break;
                    } else {
                        responses.put(
                                shards.inverse().get(task.task()), 
                                ((IMultiResponse) task.get().getRecord()).listIterator());
                    }
                }
                if (Sets.difference(shards.keySet(), responses.keySet()).isEmpty()) {
                    Map<Volume, ListIterator<Records.MultiOpRequest>> requests = Maps.newHashMapWithExpectedSize(shards.size());
                    for (Map.Entry<Volume, ShardedRequestMessage<?>> e: shards.entrySet()) {
                        requests.put(
                                e.getKey(), 
                                ((IMultiRequest) e.getValue().getRecord()).listIterator());
                    }
                    IMultiRequest multi = (IMultiRequest) task().getRecord();
                    List<Records.MultiOpResponse> ops = Lists.newArrayListWithCapacity(multi.size());
                    for (Records.MultiOpRequest op: multi) {
                        Volume v = null;
                        Records.MultiOpResponse response = null;
                        for (Map.Entry<Volume, ListIterator<Records.MultiOpRequest>> e: requests.entrySet()) {
                            if (! e.getValue().hasNext()) {
                                continue;
                            }
                            if (! op.equals(e.getValue().next())) {
                                e.getValue().previous();
                                continue;
                            }
                            if ((response == null) 
                                    || (v.getDescriptor().getRoot().prefixOf(
                                            e.getKey().getDescriptor().getRoot()))) {
                                v = e.getKey();
                                response = responses.get(v).next();
                            }
                        }
                        assert (response != null);
                        ops.add(response);
                    }
                    result = new IMultiResponse(ops);
                }
            } else {
                ResponseTask selected = null;
                for (ResponseTask task: submitted.values()) {
                    if (! task.isDone()) {
                        selected = null;
                        break;
                    } else {
                         if (selected == null) {
                            selected = task;
                        } else {
                            if ((selected.get().getRecord() instanceof Operation.Error) || (task.get().getRecord() instanceof Operation.Error)) {
                                if (task().getRecord().getOpcode() != OpCode.CLOSE_SESSION) {
                                    throw new UnsupportedOperationException();
                                }
                            } else {
                                // we should only get here for create, create2, and delete
                                // and only if the path is for a volume root
                                // in that case, pick the response that came from the volume root
                                if (shards.inverse().get(selected.task()).getDescriptor().getRoot().prefixOf(
                                        shards.inverse().get(task.task()).getDescriptor().getRoot())) {
                                    selected = task;
                                }
                            }
                        }
                    }
                }
                if (selected != null) {
                    result = selected.get().getRecord();
                }
            }
            if (result != null) {
                super.complete(result);
            }
            return this;
        }
        
        @Override
        protected boolean publish() {
            boolean published = super.publish();
            if (published) {
                for (Identifier peer: submitted.keySet()) {
                    Identifier ensemble = backends().getEnsembleForPeer().apply(peer);
                    BackendSession backend = backends().get().get(ensemble);
                    ListenableFuture<ShardedResponseMessage<?>> f = backend.poll();
                    assert (f == submitted.get(peer));
                    f = backend.peek();
                    while ((f != null) && (f.isDone()) && (! (f instanceof ResponseTask))) {
                        f = backend.poll();
                        // TODO: exception?
                        publisher.post(Futures.getUnchecked(f));
                        f = backend.peek();
                    }
                }
            }
            return published;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("task", task())
                    .add("future", delegate())
                    .add("state", state())
                    .add("volumes", volumes)
                    .add("shards", shards)
                    .add("submitted", submitted)
                    .toString();
        }

        protected class ResponseTask extends PromiseTask<ShardedRequestMessage<?>, ShardedResponseMessage<?>> implements FutureCallback<MessagePacket> {

            protected final ListenableFuture<MessagePacket> future;

            public ResponseTask(
                    ShardedRequestMessage<?> request,
                    ListenableFuture<MessagePacket> future) {
                this(request, future, SettableFuturePromise.<ShardedResponseMessage<?>>create());
            }
            
            public ResponseTask(
                    ShardedRequestMessage<?> request,
                    ListenableFuture<MessagePacket> future,
                    Promise<ShardedResponseMessage<?>> delegate) {
                super(request, delegate);
                this.future = future;
                Futures.addCallback(future, this);
            }
            
            public BackendRequestTask getRequest() {
                return BackendRequestTask.this;
            }
            
            @Override
            public boolean set(ShardedResponseMessage<?> result) {
                if (result.getXid() != task().getXid()) {
                    throw new IllegalArgumentException(result.toString());
                }
                return super.set(result);
            }
            
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean cancelled = super.cancel(mayInterruptIfRunning);
                if (cancelled) {
                    future.cancel(mayInterruptIfRunning);
                }
                return cancelled;
            }

            @Override
            public void onSuccess(MessagePacket result) {
            }

            @Override
            public void onFailure(Throwable t) {
                setException(t);
            }
        }
    }
}