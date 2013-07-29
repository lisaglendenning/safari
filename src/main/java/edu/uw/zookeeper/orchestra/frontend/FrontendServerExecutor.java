package edu.uw.zookeeper.orchestra.frontend;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.orchestra.VolumeAssignmentService;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.VolumeLookupService;
import edu.uw.zookeeper.orchestra.backend.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.backend.ShardedResponseMessage;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpen;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.server.ConnectTableProcessor;
import edu.uw.zookeeper.protocol.server.FourLetterRequestProcessor;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.protocol.server.ZxidEpochIncrementer;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpiringSessionService;
import edu.uw.zookeeper.server.ExpiringSessionTable;
import edu.uw.zookeeper.server.SessionParametersPolicy;
import edu.uw.zookeeper.server.SessionTable;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TaskExecutor;

@DependsOn({EnsembleConnectionsService.class, VolumeLookupService.class, VolumeAssignmentService.class})
public class FrontendServerExecutor extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            bind(ServerTaskExecutor.class).to(FrontendServerTaskExecutor.class).in(Singleton.class);
        }

        @Provides @Singleton
        public ExpiringSessionTable getSessionTable(
                Configuration configuration,
                Factory<Publisher> publishers,
                ScheduledExecutorService executor,
                ServiceMonitor monitor) {
            SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(configuration);
            ExpiringSessionTable sessions = ExpiringSessionTable.newInstance(publishers.get(), policy);
            ExpiringSessionService expires = ExpiringSessionService.newInstance(sessions, executor, configuration);   
            monitor.add(expires);
            return sessions;
        }

        @Provides @Singleton
        public FrontendServerExecutor getServerExecutor(
                ServiceLocator locator,
                VolumeLookupService volumes,
                VolumeAssignmentService assignments,
                EnsembleConnectionsService peers,
                Executor executor,
                ExpiringSessionTable sessions,
                DependentServiceMonitor monitor) {
            ZxidEpochIncrementer zxids = ZxidEpochIncrementer.fromZero();
            return monitor.add(FrontendServerExecutor.newInstance(
                    volumes, assignments, peers, executor, sessions, zxids, locator));
        }

        @Provides @Singleton
        public FrontendServerTaskExecutor getServerTaskExecutor(
                FrontendServerExecutor server) {
            return server.asTaskExecutor();
        }
    }
    
    public static FrontendServerExecutor newInstance(
            VolumeLookupService volumes,
            VolumeAssignmentService assignments,
            EnsembleConnectionsService peers,
            Executor executor,
            SessionTable sessions,
            Generator<Long> zxids,
            ServiceLocator locator) {
        ConcurrentMap<Long, FrontendSessionExecutor> handlers = new MapMaker().makeMap();
        FrontendServerTaskExecutor server = FrontendServerTaskExecutor.newInstance(handlers, volumes, assignments, peers, executor, sessions, zxids);
        return new FrontendServerExecutor(handlers, server, locator);
    }

    protected static CachedFunction<Identifier, Connection<MessagePacket>> getConnectionForVolume(
            final CachedFunction<Identifier, Identifier> assignments, 
            final CachedFunction<Identifier, ? extends Connection<MessagePacket>> connections) {
        return CachedFunction.create(
                new Function<Identifier, Connection<MessagePacket>>() {
                    @Override
                    public Connection<MessagePacket> apply(Identifier volume) {
                        Identifier ensemble = assignments.first().apply(volume);
                        if (ensemble != null) {
                            return connections.first().apply(ensemble);
                        } else {
                            return null;
                        }
                    }
                }, 
                new AsyncFunction<Identifier, Connection<MessagePacket>>() {
                    @Override
                    public ListenableFuture<Connection<MessagePacket>> apply(Identifier volume) throws Exception {
                        return Futures.transform(assignments.apply(volume), connections);
                    }
                });
    }
    
    protected final ServiceLocator locator;
    protected final FrontendServerTaskExecutor executor;
    protected final ConcurrentMap<Long, FrontendSessionExecutor> handlers;
    
    protected FrontendServerExecutor(
            ConcurrentMap<Long, FrontendSessionExecutor> handlers,
            FrontendServerTaskExecutor executor,
            ServiceLocator locator) {
        this.locator = locator;
        this.handlers = handlers;
        this.executor = executor;
        
        new ClientPeerConnectionListener(locator().getInstance(PeerConnectionsService.class));
    }
    
    public FrontendServerTaskExecutor asTaskExecutor() {
        return executor;
    }
    
    @Override
    protected ServiceLocator locator() {
        return locator;
    }

    protected static class FrontendServerTaskExecutor extends ServerTaskExecutor {
        public static FrontendServerTaskExecutor newInstance(
                ConcurrentMap<Long, FrontendSessionExecutor> handlers,
                VolumeLookupService volumes,
                VolumeAssignmentService assignments,
                EnsembleConnectionsService connections,
                Executor executor,
                SessionTable sessions,
                Generator<Long> zxids) {
            TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor = 
                    ServerTaskExecutor.ProcessorExecutor.of(
                            FourLetterRequestProcessor.getInstance());
            ConnectTaskExecutor connectExecutor = new ConnectTaskExecutor(
                    handlers,
                    volumes,
                    assignments,
                    connections,
                    ConnectTableProcessor.create(sessions, zxids),
                    executor);
            SessionTaskExecutor sessionExecutor = 
                    new SessionTaskExecutor(handlers);
            return new FrontendServerTaskExecutor(
                    anonymousExecutor,
                    connectExecutor,
                    sessionExecutor);
        }
        
        public FrontendServerTaskExecutor(
                TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
                ConnectTaskExecutor connectExecutor,
                SessionTaskExecutor sessionExecutor) {
            super(anonymousExecutor, connectExecutor, sessionExecutor);
        }
        
        @Override
        public ConnectTaskExecutor getConnectExecutor() {
            return (ConnectTaskExecutor) connectExecutor;
        }

        @Override
        public SessionTaskExecutor getSessionExecutor() {
            return (SessionTaskExecutor) sessionExecutor;
        }
    }
    
    protected static class SessionTaskExecutor implements TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> {

        protected final ConcurrentMap<Long, FrontendSessionExecutor> handlers;

        public SessionTaskExecutor(
                ConcurrentMap<Long, FrontendSessionExecutor> handlers) {
            this.handlers = handlers;
        }
        
        @Override
        public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
                SessionOperation.Request<Records.Request> request) {
            FrontendSessionExecutor executor = handlers.get(request.getSessionId());
            // TODO: remove handler from map on disconnect!
            return executor.submit(ProtocolRequestMessage.of(request.getXid(), request.getRecord()));
        }
        
    }

    protected static class ConnectTaskExecutor implements TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> {

        protected final ConcurrentMap<Long, FrontendSessionExecutor> handlers;
        protected final EnsembleConnectionsService connections;
        protected final Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> processor;
        protected final CachedFunction<ZNodeLabel.Path, Volume> volumeLookup;
        protected final CachedFunction<Identifier, ? extends Connection<MessagePacket>> connectionLookup;
        protected final Executor executor;
        
        public ConnectTaskExecutor(
                ConcurrentMap<Long, FrontendSessionExecutor> handlers,
                VolumeLookupService volumes,
                VolumeAssignmentService assignments,
                EnsembleConnectionsService connections,
                Processor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> processor,
                Executor executor) {
            this.connections = connections;
            this.volumeLookup = volumes.lookup();
            this.connectionLookup = getConnectionForVolume(assignments.lookup(), connections.getConnectionForEnsemble());
            this.processor = processor;
            this.executor = executor;
            this.handlers = handlers;
        }
        
        @Override
        public ListenableFuture<ConnectMessage.Response> submit(
                Pair<ConnectMessage.Request, Publisher> input) {
            try {
                ConnectMessage.Response output = processor.apply(input.first());
                if (output instanceof ConnectMessage.Response.Valid) {
                    long sessionId = output.getSessionId();
                    handlers.putIfAbsent(
                            sessionId, 
                            new FrontendSessionExecutor(
                                    sessionId, 
                                    input.second(),
                                    volumeLookup,
                                    connectionLookup,
                                    executor));
                    MessagePacket message = MessagePacket.of(MessageSessionOpen.of(sessionId, output.getTimeOut()));
                    for (Identifier ensemble: connections) {
                        ClientPeerConnection connection = connections.getConnectionForEnsemble().first().apply(ensemble);
                        if (connection != null) {
                            connection.write(message);
                        }
                    }
                }
                input.second().post(output);
                return Futures.immediateFuture(output);
            } catch (Exception e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
        
    }
    
    protected class ClientPeerConnectionListener {
        
        public ClientPeerConnectionListener(
                PeerConnectionsService<?> connections) {
            connections.clients().register(this);
        }
        
        @Subscribe
        public void handleConnection(ClientPeerConnection connection) {
            new ClientPeerConnectionDispatcher(connection);
        }
    }
    
    protected class ClientPeerConnectionDispatcher {
        protected final ClientPeerConnection connection;
        
        public ClientPeerConnectionDispatcher(
                ClientPeerConnection connection) {
            this.connection = connection;
            
            connection.register(this);
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                for (FrontendSessionExecutor handler: handlers.values()) {
                    handler.handleTransition(event);
                }
                try {
                    connection.unregister(this);
                } catch (IllegalArgumentException e) {}
            }
        }
        
        @Subscribe
        public void handleMessage(MessagePacket message) {
            switch (message.first().type()) {
            case MESSAGE_TYPE_SESSION_RESPONSE:
            {
                MessageSessionResponse body = message.getBody(MessageSessionResponse.class);
                FrontendSessionExecutor handler = handlers.get(body.getSessionId());
                handler.handleResponse(body.getResponse());
                break;
            }
            default:
                break;
            }
        }
    }

    // FIXME: disconnect
    public static class FrontendSessionExecutor extends AbstractActor<FrontendSessionExecutor.RequestTask> implements TaskExecutor<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> {

        public static List<ShardedRequestMessage<Records.Request>> shard(Message.ClientRequest<Records.Request> request, Set<Volume> volumes) {
            List<ShardedRequestMessage<Records.Request>> sharded = Lists.newArrayListWithExpectedSize(Math.max(1, volumes.size()));
            if (OpCode.MULTI == request.getRecord().getOpcode()) {
                Map<Identifier, IMultiRequest> multis = Maps.newHashMapWithExpectedSize(volumes.size());
                for (Records.MultiOpRequest op: (IMultiRequest) request.getRecord()) {
                    ZNodeLabel.Path[] paths = PathsOfRequest.getPathsOfRequest(op);
                    assert (paths.length > 0);
                    for (ZNodeLabel.Path path: paths) {
                        IMultiRequest multi = null;
                        for (Volume v: volumes) {
                            if (v.getDescriptor().contains(path)) {
                                multi = multis.get(v.getId());
                                if (multi == null) {
                                    multi = new IMultiRequest();
                                    multis.put(v.getId(), multi);
                                }
                                break;
                            }
                        }
                        if (multi == null) {
                            throw new IllegalArgumentException(request.toString());
                        } else {
                            multi.add(op);
                        }
                    }
                }
                for (Map.Entry<Identifier, IMultiRequest> multi: multis.entrySet()) {
                    sharded.add(ShardedRequestMessage.of(
                                multi.getKey(), 
                                ProtocolRequestMessage.of(
                                        request.getXid(), 
                                        (Records.Request) multi.getValue())));                            
                }
            } else {
                for (Volume v: volumes) {
                    sharded.add(ShardedRequestMessage.of(v.getId(), request));                            
                }
            }
            return sharded;
        }
        
        protected final long sessionId;
        protected final Publisher publisher;
        protected final CachedFunction<ZNodeLabel.Path, Volume> volumes;
        protected final CachedFunction<Identifier, ? extends Connection<MessagePacket>> connections;
        protected final Queue<ShardedResponseMessage<?>> responses;

        protected FrontendSessionExecutor(
                long sessionId,
                Publisher publisher,
                CachedFunction<ZNodeLabel.Path, Volume> volumes,
                CachedFunction<Identifier, ? extends Connection<MessagePacket>> connections,
                Executor executor) {
            super(executor, FutureQueue.<RequestTask>create(), AbstractActor.newState());
            this.sessionId = sessionId;
            this.publisher = publisher;
            this.volumes = volumes;
            this.connections = connections;
            this.responses = new ConcurrentLinkedQueue<ShardedResponseMessage<?>>();
        }

        @Override
        public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
                Message.ClientRequest<Records.Request> request) {
            Promise<Message.ServerResponse<Records.Response>> promise = SettableFuturePromise.create();
            RequestTask task = new RequestTask(request, promise);
            send(task);
            return task;
        }
        
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                // FIXME
            }
        }
        
        public void handleResponse(ShardedResponseMessage<?> message) {
            if (state.get() == State.TERMINATED) {
                throw new IllegalStateException();
            }
            responses.add(message);
            schedule();
        }

        @Override
        public void send(RequestTask message) {
            super.send(message);
            message.addListener(this, MoreExecutors.sameThreadExecutor());
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
            RequestTask prev = null;
            for (RequestTask task: mailbox) {
                task.apply(prev);
                prev = task;
            }
            
            ShardedResponseMessage<?> response = null;
            while ((response = responses.poll()) != null) {
                int xid = response.getXid();
                if (OpCodeXid.has(xid)) {
                    publisher.post(response.getResponse());
                } else {
                    prev = null;
                    for (RequestTask request: mailbox) {
                        if (request.handleResponse(response)) {
                            request.apply(prev);
                            break;
                        }
                        prev = request;
                    }
                    
                    super.doRun();
                }
            }
            
            super.doRun();
        }
        
        @Override
        protected boolean apply(RequestTask input) throws Exception {
            boolean running = super.apply(input);
            input.publish();
            return running;
        }
        
        protected static enum RequestState {
            NEW, LOOKING, CONNECTING, SUBMITTING, COMPLETE, PUBLISHED;
        }

        protected class RequestTask extends PromiseTask<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> implements Stateful<RequestState>, Runnable {

            protected final AtomicReference<RequestState> state;
            // TODO: or immutablelist?
            protected final ZNodeLabel.Path[] paths;
            // TODO: atomicreferencearray ?
            protected final ListenableFuture<Volume>[] lookups;
            protected final AtomicReference<ListenableFuture<List<Volume>>> lookupFuture;
            protected final AtomicReference<Map<Volume, ListenableFuture<? extends Connection<MessagePacket>>>> connectFuture;
            protected final ConcurrentMap<Identifier, ResponseTask> submitted;
            protected volatile ListenableFuture<List<ShardedResponseMessage<Records.Response>>> submittedFuture;
            
            @SuppressWarnings("unchecked")
            public RequestTask(
                    Message.ClientRequest<Records.Request> task,
                    Promise<Message.ServerResponse<Records.Response>> delegate) {
                super(task, delegate);
                this.state = new AtomicReference<RequestState>(RequestState.NEW);
                this.paths = PathsOfRequest.getPathsOfRequest(task.getRecord());
                if (paths.length > 1) {
                    Arrays.sort(paths);
                }
                this.lookups = new ListenableFuture[paths.length];
                this.lookupFuture = new AtomicReference<ListenableFuture<List<Volume>>>((paths.length == 0) ?
                    Futures.immediateFuture((List<Volume>) ImmutableList.<Volume>of()) : null);
                this.connectFuture = new AtomicReference<Map<Volume, ListenableFuture<? extends Connection<MessagePacket>>>>(null);
                this.submitted = new MapMaker().makeMap();
                this.submittedFuture = null;
            }
            
            @Override
            public RequestState state() {
                return state.get();
            }
            
            public ZNodeLabel.Path[] paths() {
                return paths;
            }
            
            public ListenableFuture<Volume>[] lookups() {
                return lookups;
            }
            
            public ConcurrentMap<Identifier, ResponseTask> submitted() {
                return submitted;
            }
            
            @Override
            public void run() {
                FrontendSessionExecutor.this.run();
            }
            
            public boolean handleResponse(ShardedResponseMessage<?> response) {
                if (task().getXid() != response.getXid()) {
                    return false;
                }
                ResponseTask task = submitted().get(response.getId());
                if (task == null) {
                    // FIXME
                    throw new AssertionError();
                }
                if (! task.set(response)) {
                    // FIXME
                    throw new AssertionError();
                }
                return true;
            }
            
            public ListenableFuture<Message.ServerResponse<Records.Response>> apply(RequestTask prev) throws Exception {
                switch (state()) {
                case NEW:
                {
                    ListenableFuture<List<Volume>> lookupFuture = lookup();
                    if (! lookupFuture.isDone()) {
                        lookupFuture.addListener(this, MoreExecutors.sameThreadExecutor());
                        break;
                    }
                }
                case LOOKING:
                {
                    ListenableFuture<List<Volume>> lookupFuture = lookup();
                    if (! lookupFuture.isDone()) {
                        break;
                    }
                    Map<Volume, ListenableFuture<? extends Connection<MessagePacket>>> connectFuture = connect();
                    ListenableFuture<List<Connection<MessagePacket>>> futures = Futures.successfulAsList(connectFuture.values());
                    if (! futures.isDone()) {
                        futures.addListener(this, MoreExecutors.sameThreadExecutor());
                        break;
                    }
                }
                case CONNECTING:
                {
                    if ((prev != null) && (prev.state().compareTo(RequestState.SUBMITTING) < 0)) {
                        // we need to preserve ordering of requests per volume
                        // as a proxy for this requirement, 
                        // don't submit until the task before us has submitted
                        // (note this is stronger than necessary)
                        break;
                    }
                    boolean done = true;
                    Map<Volume, ListenableFuture<? extends Connection<MessagePacket>>> connectFuture = connect();
                    for (ListenableFuture<? extends Connection<MessagePacket>> f: connectFuture.values()) {
                        if (! f.isDone()) {
                            done = false;
                            break;
                        }
                    }
                    if (! done) {
                        break;
                    }
                    
                    ConcurrentMap<Identifier, ResponseTask> submitted = submit();
                    ListenableFuture<List<ShardedResponseMessage<?>>> futures = Futures.successfulAsList(submitted.values());
                    if (! futures.isDone()) {
                        futures.addListener(this, MoreExecutors.sameThreadExecutor());
                        break;
                    }
                }
                case SUBMITTING:
                {
                    complete();
                }
                default:
                    break;
                }
                
                return this;
            }
            
            protected ListenableFuture<List<Volume>> lookup() throws Exception {
                this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
                // TODO: Arrays.fill(null) if there was a failure to start over...
                boolean changed = false;
                if (paths.length > 0) {
                    for (int i=0; i< paths.length; ++i) {
                        if (lookups[i] == null) {
                            changed = true;
                            lookups[i] = volumes.apply(paths[i]);
                        }
                    }
                }
                ListenableFuture<List<Volume>> prev = lookupFuture.get();
                if (changed || prev == null) {
                    lookupFuture.compareAndSet(prev, Futures.successfulAsList(lookups));
                }
                return lookupFuture.get();
            }
            
            protected Map<Volume, ListenableFuture<? extends Connection<MessagePacket>>> connect() throws Exception {
                this.state.compareAndSet(RequestState.LOOKING, RequestState.CONNECTING);
                if (connectFuture.get() == null) {
                    Map<Volume, ListenableFuture<? extends Connection<MessagePacket>>> connecting = Maps.<Volume, ListenableFuture<? extends Connection<MessagePacket>>>newHashMap();
                    try {
                        for (Volume v: lookupFuture.get().get()) {
                            if (v == null) {
                                // FIXME
                                throw new AssertionError();
                            } else if (! connecting.containsKey(v)){
                                connecting.put(v, connections.apply(v.getId()));
                            }
                        }
                    } catch (Exception e) {
                        // FIXME:
                        throw e;
                    }
                    connectFuture.compareAndSet(null, connecting);
                }
                return connectFuture.get();
            }
            
            protected ConcurrentMap<Identifier, ResponseTask> submit() throws Exception {
                this.state.compareAndSet(RequestState.CONNECTING, RequestState.SUBMITTING);
                Map<Volume, ListenableFuture<? extends Connection<MessagePacket>>> connectFuture = connect();
                List<ShardedRequestMessage<Records.Request>> requests = shard(task(), connectFuture.keySet());
                for (ShardedRequestMessage<Records.Request> request: requests) {
                    MessagePacket message = MessagePacket.of(MessageSessionRequest.of(sessionId, request));
                    ListenableFuture<MessagePacket> future = connectFuture.get(request.getId()).get().write(message);
                    ResponseTask task = new ResponseTask(request, future);
                    submitted.put(request.getId(), task);
                }
                return submitted;
            }
            
            @SuppressWarnings("unchecked")
            protected ListenableFuture<Message.ServerResponse<Records.Response>> complete() throws InterruptedException, ExecutionException {
                // FIXME
                Message.ServerResponse<Records.Response> result = null;
                for (ResponseTask response: submitted.values()) {
                    if (! response.isDone()) {
                        result = null;
                        break;
                    } else {
                        result = (Message.ServerResponse<Records.Response>) response.get();
                    }
                }
                if (result != null) {
                    this.state.compareAndSet(RequestState.SUBMITTING, RequestState.COMPLETE);
                    set(result);
                }
                return this;
            }
            
            public void publish() throws InterruptedException, ExecutionException {
                if (this.state.compareAndSet(RequestState.COMPLETE, RequestState.PUBLISHED)) {
                    publisher.post(get());
                }
            }
        }

        protected static class ResponseTask extends PromiseTask<ShardedRequestMessage<Records.Request>, ShardedResponseMessage<?>> implements Runnable {

            protected final ListenableFuture<MessagePacket> future;

            public ResponseTask(
                    ShardedRequestMessage<Records.Request> request,
                    ListenableFuture<MessagePacket> future) {
                this(request, future, SettableFuturePromise.<ShardedResponseMessage<?>>create());
            }
            
            public ResponseTask(
                    ShardedRequestMessage<Records.Request> request,
                    ListenableFuture<MessagePacket> future,
                    Promise<ShardedResponseMessage<?>> delegate) {
                super(request, delegate);
                this.future = future;
                future.addListener(this, MoreExecutors.sameThreadExecutor());
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
            public void run() {
                if (future.isDone()) {
                    try {
                        future.get();
                    } catch (Throwable t) {
                        setException(t);
                    }
                }
            }
        }
    }
}
