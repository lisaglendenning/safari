package edu.uw.zookeeper.safari.backend;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;
import edu.uw.zookeeper.safari.peer.protocol.JacksonSerializer;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

@DependsOn({BackendConnectionsService.class})
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
                VolumeCacheService volumes,
                ScheduledExecutorService executor) throws Exception {
            return BackendRequestService.newInstance(
                    injector, volumes, connections, peers, executor);
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(BackendConnectionsService.module());
        }
    }
    
    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> BackendRequestService<C> newInstance(
            Injector injector,
            VolumeCacheService volumes,
            BackendConnectionsService<C> connections,
            ServerPeerConnections peers,
            ScheduledExecutorService executor) {
        BackendRequestService<C> instance = new BackendRequestService<C>(
                injector,
                connections, 
                peers, 
                newVolumePathLookup(), 
                CachedLookup.sharedAndAdded(ShardedOperationTranslators.of(
                        volumes.byId())),
                executor);
        instance.new Advertiser(injector, MoreExecutors.sameThreadExecutor());
        return instance;
    }
    
    public static Function<ZNodeLabel.Path, Identifier> newVolumePathLookup() {
        return new Function<ZNodeLabel.Path, Identifier>() {
            @Override
            public Identifier apply(ZNodeLabel.Path input) {
                return BackendSchema.Volumes.Root.getShard(input);
            }
        };
    }
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();

    protected final Logger logger;
    protected final BackendConnectionsService<C> connections;
    protected final ConcurrentMap<ServerPeerConnection<?>, ServerPeerConnectionListener> peers;
    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final CachedLookup<Identifier, OperationPrefixTranslator> translator;
    protected final ServerPeerConnectionListener listener;
    protected final ScheduledExecutorService executor;
    
    protected BackendRequestService(
            Injector injector,
            BackendConnectionsService<C> connections,
            ServerPeerConnections peers,
            Function<ZNodeLabel.Path, Identifier> lookup,
            CachedLookup<Identifier, OperationPrefixTranslator> translator,
            ScheduledExecutorService executor) {
        super(injector);
        this.logger = LogManager.getLogger(getClass());
        this.connections = connections;
        this.lookup = lookup;
        this.translator = translator;
        this.executor = executor;
        this.peers = new MapMaker().makeMap();
        this.listener = new ServerPeerConnectionListener(peers);
    }
    
    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        OperationClientExecutor<C> client = OperationClientExecutor.newInstance(
                ConnectMessage.Request.NewRequest.newInstance(), 
                connections.get().get(),
                executor);
        Control.createPrefix(
                Materializer.newInstance(
                BackendSchema.getInstance().get(), 
                JacksonSerializer.create(injector.getInstance(ObjectMapper.class)),
                client));
        ConnectionClientExecutorService.disconnect(client);
        
        listener.start();
    }
    
    @Override
    protected void shutDown() throws Exception {
        listener.stop();
        
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
            Materializer<?> materializer = injector.getInstance(ControlMaterializerService.class).materializer();
            Identifier myEntity = injector.getInstance(PeerConfiguration.class).getView().id();
            BackendView view = injector.getInstance(BackendConfiguration.class).getView();
            try {
                BackendConfiguration.advertise(myEntity, view, materializer);
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
            Long sessionId = message.getIdentifier();
            BackendHandler client = clients.get(sessionId);
            if (client == null) {
                new SessionOpenTask(message);
            } else {
                Futures.addCallback(
                        client.client().session(), 
                        new SessionOpenResponseTask(message),
                        SAME_THREAD_EXECUTOR);
            }
        }
        
        protected void handleMessageSessionRequest(MessageSessionRequest message) {
            logger.debug("{}", message);
            BackendHandler client = clients.get(message.getIdentifier());
            if (client != null) {
                client.submit(message.getValue());
            } else {
                // FIXME
                throw new UnsupportedOperationException();
            }
        }

        protected class SessionOpenTask implements Runnable {
        
            protected final MessageSessionOpenRequest session;
            protected final ListenableFuture<? extends C> connection;
            
            public SessionOpenTask(MessageSessionOpenRequest session) {
                this.session = session;
                this.connection = connections.get();
                this.connection.addListener(this, SAME_THREAD_EXECUTOR);
            }
            
            @Override
            public void run() {
                if (this.connection.isDone()) {
                    try {
                        C connection = this.connection.get();
                        ConnectMessage.Request request;
                        if (session.getValue() instanceof ConnectMessage.Request.NewRequest) {
                            request = ConnectMessage.Request.NewRequest.newInstance(
                                    TimeValue.milliseconds(session.getValue().getTimeOut()), 
                                        connections.zxids().get());
                        } else {
                            request = ConnectMessage.Request.RenewRequest.newInstance(
                                    session.getValue().toSession(), connections.zxids().get());
                        }
                        
                        ShardedClientExecutor<C> client = ShardedClientExecutor.newInstance(
                                lookup, 
                                translator.asLookup(), 
                                request, 
                                connection,
                                executor);
                        new BackendHandler(session, client);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        // TODO
                        throw new UnsupportedOperationException(e);
                    }
                }
            }
        }

        protected class BackendHandler implements FutureCallback<ShardedResponseMessage<?>>, SessionListener, TaskExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>> {

            protected final MessageSessionOpenRequest session;
            protected final ShardedClientExecutor<C> client;
            
            public BackendHandler(
                    MessageSessionOpenRequest session,
                    ShardedClientExecutor<C> client) {
                this.session = session;
                this.client = client;
                if (clients.putIfAbsent(session.getIdentifier(), this) == null) {
                    Futures.addCallback(
                            client.session(), 
                            new SessionOpenResponseTask(session),
                            SAME_THREAD_EXECUTOR);
                    this.client.subscribe(this);
                } else {
                    // TODO
                    throw new UnsupportedOperationException(String.valueOf(session));
                }
            }
            
            public MessageSessionOpenRequest session() {
                return session;
            }
            
            public ShardedClientExecutor<C> client() {
                return client;
            }

            @Override
            public ListenableFuture<ShardedResponseMessage<?>> submit(ShardedRequestMessage<?> request) {
                ListenableFuture<ShardedResponseMessage<?>> future = client.submit(request);
                Futures.addCallback(future, this, SAME_THREAD_EXECUTOR);
                return future;
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

        protected class SessionOpenResponseTask implements FutureCallback<ConnectMessage.Response> {

            protected final MessageSessionOpenRequest request;
            
            public SessionOpenResponseTask(
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
                throw new AssertionError(t);
            }
        }
    }
}
