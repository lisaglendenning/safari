package edu.uw.zookeeper.safari.backend;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ServerResponse;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.PrefixCreator;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;
import edu.uw.zookeeper.safari.peer.protocol.JacksonSerializer;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.ShardedClientRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedErrorResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;

@DependsOn({BackendConnectionsService.class, VersionedVolumeCacheService.class})
public class BackendRequestService<C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            TypeLiteral<BackendRequestService<?>> generic = new TypeLiteral<BackendRequestService<?>>() {};
            bind(BackendRequestService.class).to(generic);
        }

        @Provides @Singleton
        public BackendRequestService<?> getBackendRequestService(
                Injector injector,
                BackendConnectionsService<?> connections,
                ServerPeerConnections peers,
                VersionedVolumeCacheService volumes,
                ScheduledExecutorService scheduler) throws Exception {
            return BackendRequestService.newInstance(
                    injector, volumes, connections, peers, scheduler);
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(
                    VersionedVolumeCacheService.module(),
                    BackendConnectionsService.module());
        }
    }
    
    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> BackendRequestService<C> newInstance(
            Injector injector,
            VersionedVolumeCacheService volumes,
            BackendConnectionsService<C> connections,
            ServerPeerConnections peers,
            ScheduledExecutorService scheduler) {
        BackendRequestService<C> instance = new BackendRequestService<C>(
                injector,
                volumes,
                connections, 
                peers,
                scheduler);
        instance.new Advertiser(injector, SameThreadExecutor.getInstance());
        return instance;
    }

    protected final Logger logger;
    protected final BackendConnectionsService<C> connections;
    protected final ConcurrentMap<ServerPeerConnection<?>, ServerPeerConnectionListener> peers;
    protected final ServerPeerConnectionListener listener;
    protected final ScheduledExecutorService scheduler;
    protected final VersionedVolumeCacheService volumes;
    protected Materializer<BackendZNode<?>,Message.ServerResponse<?>> materializer;
    
    protected BackendRequestService(
            Injector injector,
            VersionedVolumeCacheService volumes,
            BackendConnectionsService<C> connections,
            ServerPeerConnections peers,
            ScheduledExecutorService scheduler) {
        super(injector);
        this.logger = LogManager.getLogger(getClass());
        this.volumes = volumes;
        this.connections = connections;
        this.scheduler = scheduler;
        this.peers = new MapMaker().makeMap();
        this.listener = new ServerPeerConnectionListener(peers);
        this.materializer = null;
    }
    
    public Materializer<BackendZNode<?>,ServerResponse<?>> get() {
        return materializer;
    }
    
    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        OperationClientExecutor<C> client = OperationClientExecutor.newInstance(
                ConnectMessage.Request.NewRequest.newInstance(injector.getInstance(BackendConfiguration.class).getTimeOut(), 0L), 
                connections.get().get(),
                scheduler);
        this.materializer = Materializer.<BackendZNode<?>,Message.ServerResponse<?>>fromHierarchy(
                BackendSchema.class, 
                JacksonSerializer.create(injector.getInstance(ObjectMapper.class)), 
                client);
        
        Futures.successfulAsList(PrefixCreator.forMaterializer(materializer).call()).get();
        
        listener.start();
    }
    
    @Override
    protected void shutDown() throws Exception {
        listener.stop();
        
        ConnectionClientExecutorService.disconnect((OperationClientExecutor<?>) materializer.cache().client());
        
        super.shutDown();
    }
    
    public class Advertiser extends Service.Listener {

        protected final Injector injector;
        
        public Advertiser(Injector injector, Executor executor) {
            this.injector = injector;
            addListener(this, executor);
        }
        
        @Override
        public void running() {
            Materializer<ControlZNode<?>,?> materializer = injector.getInstance(ControlMaterializerService.class).materializer();
            Identifier myEntity = injector.getInstance(PeerConfiguration.class).getView().id();
            BackendView view = injector.getInstance(BackendConfiguration.class).getView();
            try {
                BackendConfiguration.advertise(myEntity, view, materializer).get();
            } catch (Exception e) {
                logger.warn("", e);
                injector.getInstance(BackendRequestService.class).stopAsync();
            }
        }
    }

    protected class ServerPeerConnectionListener implements ConnectionFactory.ConnectionsListener<ServerPeerConnection<?>>{
        
        protected final ServerPeerConnections connections;
        protected final ConcurrentMap<ServerPeerConnection<?>, ServerPeerConnectionDispatcher> dispatchers;
        
        public ServerPeerConnectionListener(
                ServerPeerConnections connections) {
            this.connections = connections;
            this.dispatchers = new MapMaker().makeMap();
        }
        
        public void start() {
            connections.subscribe(this);
            for (ServerPeerConnection<?> c: connections) {
                handleConnectionOpen(c);
            }
        }
        
        public void stop() {
            try {
                connections.unsubscribe(this);
            } catch (IllegalArgumentException e) {}
        }

        @Override
        public void handleConnectionOpen(ServerPeerConnection<?> connection) {
            ServerPeerConnectionDispatcher d = new ServerPeerConnectionDispatcher(connection);
            if (dispatchers.putIfAbsent(connection, d) == null) {
                connection.subscribe(d);
            }
        }
    }
    
    @SuppressWarnings("rawtypes")
    protected class ServerPeerConnectionDispatcher extends Factories.Holder<ServerPeerConnection<?>> implements Connection.Listener<MessagePacket> {

        protected final ConcurrentMap<Long, BackendHandler> clients;
        
        public ServerPeerConnectionDispatcher(ServerPeerConnection<?> connection) {
            super(connection);
            this.clients = new MapMaker().makeMap();
        }

        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            if (Connection.State.CONNECTION_CLOSED == state.to()) {
                get().unsubscribe(this);
                listener.dispatchers.remove(get(), this);
            }
        }

        @Override
        public void handleConnectionRead(MessagePacket message) {
            logger.debug("{}", message);
            switch (message.getHeader().type()) {
            case MESSAGE_TYPE_HANDSHAKE:
            case MESSAGE_TYPE_HEARTBEAT:
                break;
            case MESSAGE_TYPE_SESSION_OPEN_REQUEST:
                handleMessageSessionOpen((MessageSessionOpenRequest) message.getBody());
                break;
            case MESSAGE_TYPE_SESSION_REQUEST:
                handleMessageSessionRequest((MessageSessionRequest) message.getBody());
                break;
            default:
                throw new AssertionError(message.toString());
            }
        }
        
        protected void handleMessageSessionOpen(MessageSessionOpenRequest message) {
            SessionOpenTask task = new SessionOpenTask(message, SettableFuturePromise.<ConnectMessage.Response>create());
            Futures.addCallback(
                    task, 
                    new SessionOpenListener(message),
                    SameThreadExecutor.getInstance());
            task.run();
        }
        
        protected void handleMessageSessionRequest(MessageSessionRequest message) {
            BackendHandler client = clients.get(message.getIdentifier());
            if (client != null) {
                client.submit((ShardedClientRequestMessage<?>) message.getMessage());
            } else {
                // FIXME
                throw new UnsupportedOperationException();
            }
        }

        protected class SessionOpenTask extends PromiseTask<MessageSessionOpenRequest,ConnectMessage.Response> implements FutureCallback<ConnectMessage.Response>, Runnable, Callable<Optional<ConnectMessage.Response>> {
        
            private final CallablePromiseTask<SessionOpenTask, ConnectMessage.Response> delegate;
            private Optional<SessionOpenToConnectRequest> request = Optional.absent();
            private Optional<ListenableFuture<C>> connection = Optional.absent();
            private Optional<RegisterSessionTask> session = Optional.absent();
            
            public SessionOpenTask(
                    MessageSessionOpenRequest task,
                    Promise<ConnectMessage.Response> promise) {
                super(task, promise);
                this.delegate = CallablePromiseTask.create(this, this);
            }

            @Override
            public void onSuccess(ConnectMessage.Response result) {
                set(result);
            }

            @Override
            public void onFailure(Throwable t) {
                setException(t);
            }
            
            @Override
            public synchronized void run() {
                delegate.run();
            }
            
            @Override
            public synchronized Optional<ConnectMessage.Response> call() throws Exception {
                if (!request.isPresent()) {
                    BackendHandler handler = clients.get(task.getIdentifier());
                    if (handler != null) {
                        Futures.addCallback(handler.client().session(), this, SameThreadExecutor.getInstance());
                    } else {
                        this.request = Optional.of(new SessionOpenToConnectRequest(task, SettableFuturePromise.<ConnectMessage.Request>create()));
                        this.request.get().addListener(this, SameThreadExecutor.getInstance());
                    }
                    return Optional.absent();
                }
                if (request.get().isDone()) {
                    if (!connection.isPresent()) {
                        connection = Optional.of(connections.get());
                        connection.get().addListener(this, SameThreadExecutor.getInstance());
                    } else {
                        if (connection.get().isDone()) {
                            if (! session.isPresent()) {
                                ConnectTask<C> task;
                                try {
                                    task = ConnectTask.connect( 
                                            request.get().get(),
                                            connection.get().get());
                                } catch (ExecutionException e) {
                                    throw Throwables.propagate(e.getCause());
                                } catch (InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                                session = Optional.of(new RegisterSessionTask(task, delegate()));
                            }
                        }
                    }
                }
                return Optional.absent();
            }
            
            protected class RegisterSessionTask extends PromiseTask<ConnectTask<C>,ConnectMessage.Response> implements Runnable, FutureCallback<Object>, Connection.Listener<Operation.Response> {
                
                private Optional<? extends ListenableFuture<? extends Message.ClientRequest<?>>> create = Optional.absent();
                
                public RegisterSessionTask(
                        ConnectTask<C> task,
                        Promise<ConnectMessage.Response> promise) {
                    super(task, promise);
                    task().addListener(this, SameThreadExecutor.getInstance());
                    promise.addListener(this, SameThreadExecutor.getInstance());
                }
                
                @Override
                public synchronized void run() {
                    if (isDone()) {
                        task().task().second().unsubscribe(this);
                    } else if (task().isDone()) {
                        if (!create.isPresent()) {
                            try {
                                final BackendSchema.Safari.Sessions.Session.Data value = BackendSchema.Safari.Sessions.Session.Data.valueOf(task().get().getSessionId(), task().get().getPasswd());
                                final ZNodePath path = BackendSchema.Safari.Sessions.Session.pathOf(SessionOpenTask.this.task().getIdentifier());
                                // FIXME: magic constant xid
                                final Message.ClientRequest<?> request = ProtocolRequestMessage.of(0, materializer.create(path, value).get().build());
                                task().task().second().subscribe(this);
                                this.create = Optional.of(task().task().second().write(request));
                                Futures.addCallback(create.get(), this, SameThreadExecutor.getInstance());
                            } catch (ExecutionException e) {
                                setException(e.getCause());
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                        }
                    }
                }

                @Override
                public void handleConnectionState(Automaton.Transition<edu.uw.zookeeper.net.Connection.State> state) {
                    if (Connection.State.CONNECTION_CLOSED == state.to()) {
                        onFailure(new KeeperException.ConnectionLossException());
                    }
                }

                @Override
                public void handleConnectionRead(Operation.Response message) {
                    assert (((Operation.ProtocolResponse<?>) message).xid() == 0);
                    try {
                        Operations.unlessError(((Operation.ProtocolResponse<?>) message).record());
                    } catch (KeeperException e) {
                        setException(e);
                        return;
                    }
                    ShardedClientExecutor<C> client = ShardedClientExecutor.newInstance(
                            volumes.idToVersion(),
                            volumes.idToZxid(),
                            volumes.idToVolume(),
                            volumes.idToPath(),
                            task(), 
                            task().task().second(),
                            TimeValue.milliseconds(task().task().first().getTimeOut()),
                            scheduler);
                    BackendHandler handler = new BackendHandler(SessionOpenTask.this.task(), client);
                    if (clients.putIfAbsent(handler.session().getIdentifier(), handler) != null) {
                        // FIXME
                        throw new UnsupportedOperationException();
                    }
                    onSuccess(Futures.getUnchecked(task()));
                }

                @Override
                public void onSuccess(Object result) {
                    if (result instanceof ConnectMessage.Response) {
                        set((ConnectMessage.Response) result);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    setException(t);
                }
            }
        }
        
        protected class BackendHandler implements FutureCallback<ShardedResponseMessage<?>>, SessionListener, TaskExecutor<ShardedClientRequestMessage<?>, ShardedResponseMessage<?>> {

            protected final MessageSessionOpenRequest session;
            protected final ShardedClientExecutor<C> client;
            
            public BackendHandler(
                    MessageSessionOpenRequest session,
                    ShardedClientExecutor<C> client) {
                this.session = session;
                this.client = client;
                this.client.subscribe(this);
            }
            
            public MessageSessionOpenRequest session() {
                return session;
            }
            
            public ShardedClientExecutor<C> client() {
                return client;
            }

            @Override
            public ListenableFuture<ShardedResponseMessage<?>> submit(ShardedClientRequestMessage<?> request) {
                RequestListener listener = RequestListener.create(request, client);
                Futures.addCallback(listener, this, SameThreadExecutor.getInstance());
                return listener;
            }

            @Override
            public void onSuccess(ShardedResponseMessage<?> result) {
                instance.write(MessagePacket.of(MessageSessionResponse.of(
                        session.getIdentifier(), result)));
            }

            @Override
            public void onFailure(Throwable t) {
                // FIXME
                client.unsubscribe(this);
                clients.remove(session.getIdentifier(), this);
                throw new UnsupportedOperationException(t);
            }

            @Override
            public void handleAutomatonTransition(
                    Automaton.Transition<ProtocolState> transition) {
                switch (transition.to()) {
                case DISCONNECTED:
                case ERROR:
                    client.unsubscribe(this);
                    clients.remove(session.getIdentifier(), this);
                    break;
                default:
                    break;
                }
            }

            @Override
            public void handleNotification(
                    Operation.ProtocolResponse<IWatcherEvent> notification) {
                onSuccess((ShardedResponseMessage<?>) notification);
            }
        }

        protected class SessionOpenListener implements FutureCallback<ConnectMessage.Response> {

            protected final MessageSessionOpenRequest request;
            
            public SessionOpenListener(
                    MessageSessionOpenRequest request) {
                this.request = request;
            }

            @Override
            public void onSuccess(ConnectMessage.Response result) {
                instance.write(
                        MessagePacket.of(
                                MessageSessionOpenResponse.of(
                                        request.getIdentifier(), result)));
            }
        
            @Override
            public void onFailure(Throwable t) {
                BackendHandler handler = clients.get(request.getIdentifier());
                if ((handler != null) && (handler.session() == request)) {
                    handler.onFailure(t);
                }
                // FIXME
                throw new UnsupportedOperationException(t);
            }
        }
    }

    protected class SessionOpenToConnectRequest extends PromiseTask<MessageSessionOpenRequest, ConnectMessage.Request> implements Callable<Optional<ConnectMessage.Request>> {

        private final SessionLookup lookup;
        
        public SessionOpenToConnectRequest(
                MessageSessionOpenRequest task,
                Promise<ConnectMessage.Request> promise) {
            super(task, promise);
            final ConnectMessage.Request request = task.getMessage();
            final ConnectMessage.Request message;
            if (request instanceof ConnectMessage.Request.NewRequest) {
                if ((request.getPasswd() != null) && (request.getPasswd().length > 0)) {
                    message = null;
                    this.lookup = new SessionLookup(
                            task.getIdentifier(), 
                            SettableFuturePromise.<BackendSchema.Safari.Sessions.Session.Data>create());
                } else {
                    this.lookup = null;
                    message = ConnectMessage.Request.NewRequest.newInstance(
                            TimeValue.milliseconds(request.getTimeOut()), 
                                connections.zxids().get());
                }
            } else {
                this.lookup = null;
                message = ConnectMessage.Request.RenewRequest.newInstance(
                        request.toSession(), connections.zxids().get());
            }
            if (message != null) {
                set(message);
            } else {
                this.lookup.addListener(CallablePromiseTask.create(this, this), SameThreadExecutor.getInstance());
            }
        }

        @Override
        public Optional<ConnectMessage.Request> call() throws Exception {
            if (lookup.isDone()) {
                BackendSchema.Safari.Sessions.Session.Data backend;
                try {
                    backend = lookup.get();
                } catch (ExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
                return Optional.<ConnectMessage.Request>of(
                        ConnectMessage.Request.RenewRequest.newInstance(
                        Session.create(
                                backend.getSessionId().longValue(), 
                                Session.Parameters.create(
                                        task().getMessage().toSession().parameters().timeOut(),
                                        backend.getPassword())), 
                        connections.zxids().get()));
            }
            return Optional.absent();
        }
    }
    
    protected class SessionLookup extends PromiseTask<Long, BackendSchema.Safari.Sessions.Session.Data> implements Callable<Optional<BackendSchema.Safari.Sessions.Session.Data>> {
        
        private final ListenableFuture<Message.ServerResponse<?>> lookup;
        
        public SessionLookup(Long session, 
                Promise<BackendSchema.Safari.Sessions.Session.Data> promise) {
            super(session, promise);
            // TODO check cache first
            this.lookup = materializer.submit(Operations.Requests.getData().setPath(path()).build());
            this.lookup.addListener(CallablePromiseTask.create(this, this), SameThreadExecutor.getInstance());
        }
        
        public ZNodePath path() {
            return BackendSchema.Safari.Sessions.Session.pathOf(task());
        }

        @Override
        public Optional<BackendSchema.Safari.Sessions.Session.Data> call() throws Exception {
            if (! lookup.isDone()) {
                return Optional.absent();
            }
            try {
                Operations.unlessError(lookup.get().record());
                materializer.cache().lock().readLock().lock();
                try {
                    return Optional.of((BackendSchema.Safari.Sessions.Session.Data) materializer.cache().cache().get(path()).data().get());
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
            } catch (ExecutionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
    }
    
    protected static class RequestListener extends PromiseTask<ShardedClientRequestMessage<?>, ShardedResponseMessage<?>> implements Callable<Optional<ShardedResponseMessage<?>>> {
    
        public static RequestListener create(
                ShardedClientRequestMessage<?> request,
                ShardedClientExecutor<?> client) {
            ListenableFuture<ShardedServerResponseMessage<?>> response = client.submit(request);
            RequestListener listener = new RequestListener(request, response, SettableFuturePromise.<ShardedResponseMessage<?>>create());
            response.addListener(CallablePromiseTask.create(listener, listener), SameThreadExecutor.getInstance());
            return listener;
        }
        
        private final ListenableFuture<ShardedServerResponseMessage<?>> response;
        
        public RequestListener(
                ShardedClientRequestMessage<?> request,
                ListenableFuture<ShardedServerResponseMessage<?>> response,
                Promise<ShardedResponseMessage<?>> delegate) {
            super(request, delegate);
            this.response = response;
        }

        @Override
        public Optional<ShardedResponseMessage<?>> call() throws Exception {
            if (response.isDone()) {
                ShardedResponseMessage<?> result;
                try {
                    result = response.get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof SafariException) {
                        result = ShardedErrorResponseMessage.valueOf(task().getShard(), task().xid(), (SafariException) e.getCause());
                    } else {
                        setException(e.getCause());
                        return Optional.absent();
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return Optional.<ShardedResponseMessage<?>>of(result);
            }
            return Optional.absent();
        }
    }
}
