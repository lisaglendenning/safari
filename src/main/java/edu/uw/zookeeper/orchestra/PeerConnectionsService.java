package edu.uw.zookeeper.orchestra;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmConnectionEndpoint;
import edu.uw.zookeeper.orchestra.backend.BackendRequestService;
import edu.uw.zookeeper.orchestra.backend.ShardedClientConnectionExecutor;
import edu.uw.zookeeper.orchestra.backend.ShardedResponseMessage;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.protocol.FramedMessagePacketCodec;
import edu.uw.zookeeper.orchestra.protocol.MessageHandshake;
import edu.uw.zookeeper.orchestra.protocol.JacksonModule;
import edu.uw.zookeeper.orchestra.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.protocol.MessagePacketCodec;
import edu.uw.zookeeper.orchestra.protocol.MessageSessionClose;
import edu.uw.zookeeper.orchestra.protocol.MessageSessionOpen;
import edu.uw.zookeeper.orchestra.protocol.MessageSessionRequest;
import edu.uw.zookeeper.orchestra.protocol.MessageSessionResponse;
import edu.uw.zookeeper.orchestra.protocol.MessageType;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;

public class PeerConnectionsService extends AbstractIdleService {

    public static ParameterizedFactory<Publisher, Pair<Class<MessagePacket>, FramedMessagePacketCodec>> codecFactory(
            final ObjectMapper mapper) {
        return new ParameterizedFactory<Publisher, Pair<Class<MessagePacket>, FramedMessagePacketCodec>>() {
            
            protected final Pair<Class<MessagePacket>, FramedMessagePacketCodec> codec = Pair.create(MessagePacket.class, FramedMessagePacketCodec.newInstance(MessagePacketCodec.newInstance(mapper)));
            
            @Override
            public Pair<Class<MessagePacket>, FramedMessagePacketCodec> get(
                    Publisher value) {
                return codec;
            }
        };
    }
    
    public static ParameterizedFactory<Pair<Pair<Class<MessagePacket>, FramedMessagePacketCodec>, Connection<MessagePacket>>, Connection<MessagePacket>> connectionFactory() {
        return new ParameterizedFactory<Pair<Pair<Class<MessagePacket>, FramedMessagePacketCodec>, Connection<MessagePacket>>, Connection<MessagePacket>>() {
            @Override
            public Connection<MessagePacket> get(
                    Pair<Pair<Class<MessagePacket>, FramedMessagePacketCodec>, Connection<MessagePacket>> value) {
                return value.second();
            }
        };
    }
    
    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public PeerConnectionsService getConductorPeerService(
                ConductorConfiguration configuration,
                ServiceLocator locator,
                NettyModule netModule,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            ServerConnectionFactory<MessagePacket, Connection<MessagePacket>> serverConnections = 
                    netModule.servers().get(
                            codecFactory(JacksonModule.getMapper()), 
                            connectionFactory())
                    .get(configuration.getAddress().address().get());
            runtime.serviceMonitor().addOnStart(serverConnections);
            ClientConnectionFactory<MessagePacket, Connection<MessagePacket>> clientConnections =  
                    netModule.clients().get(
                            codecFactory(JacksonModule.getMapper()), 
                            connectionFactory()).get();
            runtime.serviceMonitor().addOnStart(clientConnections);
            Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint> loopbackEndpoints = Pair.create(
                    IntraVmConnectionEndpoint.create(runtime.publisherFactory().get(), MoreExecutors.sameThreadExecutor()),
                    IntraVmConnectionEndpoint.create(runtime.publisherFactory().get(), MoreExecutors.sameThreadExecutor()));
            Pair<IntraVmConnection, IntraVmConnection> loopback = 
                    IntraVmConnection.createPair(loopbackEndpoints);
            PeerConnectionsService instance = new PeerConnectionsService(configuration.getAddress().id(), serverConnections, clientConnections, loopback, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }

    protected final Identifier identifier;
    protected final ServiceLocator locator;
    protected final ServerConnectionFactory<MessagePacket, Connection<MessagePacket>> serverConnectionFactory;
    protected final ClientConnectionFactory<MessagePacket, Connection<MessagePacket>> clientConnectionFactory;
    protected final ConcurrentMap<Identifier, ServerPeerConnection> serverConnections;
    protected final ConcurrentMap<Identifier, ClientPeerConnection> clientConnections;
    protected final Pair<ServerPeerConnection, ClientPeerConnection> loopback;
    
    public PeerConnectionsService(
            Identifier identifier,
            ServerConnectionFactory<MessagePacket, Connection<MessagePacket>> serverConnectionFactory,
            ClientConnectionFactory<MessagePacket, Connection<MessagePacket>> clientConnectionFactory,
            Pair<? extends Connection<? super MessagePacket>, ? extends Connection<? super MessagePacket>> loopback,
            ServiceLocator locator) {
        this.identifier = identifier;
        this.serverConnectionFactory = serverConnectionFactory;
        this.clientConnectionFactory = clientConnectionFactory;
        this.locator = locator;
        this.serverConnections = new ConcurrentHashMap<Identifier, ServerPeerConnection>();
        this.clientConnections = new ConcurrentHashMap<Identifier, ClientPeerConnection>();
        this.loopback = Pair.create(
                new ServerPeerConnection(identifier, loopback.first()),
                new ClientPeerConnection(identifier, loopback.second()));
    }
    
    public ClientPeerConnection getClientConnection(Identifier identifier) {
        if (this.identifier.equals(identifier)) {
            return loopback.second();
        } else {
            return clientConnections.get(identifier);
        }
    }

    public ServerPeerConnection getServerConnection(Identifier identifier) {
        if (this.identifier.equals(identifier)) {
            return loopback.first();
        } else {
            return serverConnections.get(identifier);
        }
    }
    
    public ListenableFuture<ClientPeerConnection> connect(Identifier identifier, Executor executor) {
        return new ClientConnectTask(identifier, executor);
    }
    
    @Override
    protected void startUp() throws Exception {
        new ServerAcceptListener();
        
        serverConnectionFactory.start().get();
        clientConnectionFactory.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        loopback.first().second().close().get();
        loopback.second().second().close().get();
    }

    public class PeerConnection extends Pair<Identifier, Connection<? super MessagePacket>> {
    
        public PeerConnection(Identifier first, Connection<? super MessagePacket> second) {
            super(first, second);
            
            second.register(this);
        }

        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    second().unregister(this);
                } catch (IllegalArgumentException e) {}
            }
        }
    }

    public class ClientPeerConnection extends PeerConnection {
    
        public ClientPeerConnection(Identifier first, Connection<? super MessagePacket> second) {
            super(first, second);
            
            ClientPeerConnection prev = clientConnections.putIfAbsent(first(), this);
            if (prev != null) {
                second().close();
            } else {
                second().write(MessagePacket.of(MessageHandshake.of(identifier)));
            }
        }

        @Override
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            super.handleTransition(event);
            
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                clientConnections.remove(first(), this);
            }
        }
    }
    
    public class ServerPeerConnection extends PeerConnection {
        
        public ServerPeerConnection(Identifier first, Connection<? super MessagePacket> second) {
            super(first, second);

            ServerPeerConnection prev = serverConnections.putIfAbsent(first(), this);
            if (prev != null) {
                prev.second().close();
                serverConnections.remove(prev.first(), prev);
                prev = serverConnections.putIfAbsent(first(), this);
                if (prev != null) {
                    throw new IllegalStateException();
                }
            }
        }

        @Override
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            super.handleTransition(event);
            
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                serverConnections.remove(first(), this);
            }
        }
        
        @Subscribe
        public void handlePeerMessage(MessagePacket message) {
            switch (message.first().type()) {
            case MESSAGE_TYPE_SESSION_OPEN:
                handleMessageSessionOpen((MessageSessionOpen) message.second());
                break;
            case MESSAGE_TYPE_SESSION_CLOSE:
                handleMessageSessionClose((MessageSessionClose) message.second());
                break;
            case MESSAGE_TYPE_SESSION_REQUEST:
                handleMessageSessionRequest((MessageSessionRequest) message.second());
                break;
            default:
                throw new AssertionError(message.toString());
            }
        }
        
        protected void handleMessageSessionRequest(MessageSessionRequest message) {
            ShardedClientConnectionExecutor<?> client = locator.getInstance(BackendRequestService.class).get(message.getSessionId());
            client.submit(message.getRequest());
        }

        protected void handleMessageSessionOpen(MessageSessionOpen message) {
            ShardedClientConnectionExecutor<?> client = locator.getInstance(BackendRequestService.class).get(message);
            new BackendClientListener(message.getSessionId(), client);
        }

        protected void handleMessageSessionClose(MessageSessionClose message) {
            locator.getInstance(BackendRequestService.class).remove(message);
        }
        
        protected class BackendClientListener {
            protected final long sessionId;
            protected final ShardedClientConnectionExecutor<?> client;
            
            public BackendClientListener(
                    long sessionId,
                    ShardedClientConnectionExecutor<?> client) {
                this.sessionId = sessionId;
                this.client = client;
                client.register(this);
            }

            @Subscribe
            public void handleTransition(Automaton.Transition<?> event) {
                if (Connection.State.CONNECTION_CLOSED == event.to()) {
                    try {
                        client.unregister(this);
                    } catch (IllegalArgumentException e) {}
                }
            }
            
            @Subscribe
            public void handleResponse(ShardedResponseMessage message) {
                second().write(MessagePacket.of(MessageSessionResponse.of(sessionId, message)));
            }
        }
    }
    
    protected class ClientConnectTask extends PromiseTask<Identifier, ClientPeerConnection> implements FutureCallback<Connection<MessagePacket>> {

        protected ClientConnectTask(Identifier task, Executor executor) {
            this(task, PromiseTask.<ClientPeerConnection>newPromise(), executor);
        }
        
        protected final Executor executor;
        
        protected ClientConnectTask(Identifier task,
                Promise<ClientPeerConnection> delegate, Executor executor) {
            super(task, delegate);
            this.executor = executor;
            LookupAddressTask lookupTask = new LookupAddressTask();
            ListenableFutureTask<Orchestra.Conductors.Entity.ConductorAddress> lookupFuture = ListenableFutureTask.create(lookupTask);
            Futures.addCallback(lookupFuture, lookupTask, executor);
            executor.execute(lookupFuture);
        }

        @Override
        public void onSuccess(Connection<MessagePacket> result) {
            new ClientPeerConnection(task(), result);
            set(clientConnections.get(task()));
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
        
        protected class LookupAddressTask implements Callable<Orchestra.Conductors.Entity.ConductorAddress>, FutureCallback<Orchestra.Conductors.Entity.ConductorAddress> {
            @Override
            public Orchestra.Conductors.Entity.ConductorAddress call() throws Exception {
                Materializer<?,?> materializer = locator.getInstance(ControlClientService.class).materializer();
                return Orchestra.Conductors.Entity.ConductorAddress.lookup(Orchestra.Conductors.Entity.of(task), materializer);
            }
            
            @Override
            public void onSuccess(Orchestra.Conductors.Entity.ConductorAddress result) {
                Futures.addCallback(clientConnectionFactory.connect(result.get().get()), ClientConnectTask.this, executor);
            }

            @Override
            public void onFailure(Throwable t) {
                ClientConnectTask.this.setException(t);
            }
        }
    }

    protected class ServerAcceptListener {
        protected ServerAcceptListener() {
            serverConnectionFactory.register(this);
        }

        @Subscribe
        public void handleServerConnection(Connection<MessagePacket> connection) {
            new ServerAcceptTask(connection);
        }
    }
        
    protected class ServerAcceptTask {

        protected final Connection<MessagePacket> connection;
        
        protected ServerAcceptTask(Connection<MessagePacket> connection) {
            this.connection = connection;
            
            connection.register(this);
        }
        
        @Subscribe
        public void handleMessage(MessagePacket event) {
            if (MessageType.MESSAGE_TYPE_HANDSHAKE == event.first().type()) {
                MessageHandshake body = (MessageHandshake) event.second();
                new ServerPeerConnection(body.getId(), connection);
                connection.unregister(this);
            } else {
                throw new AssertionError(event.toString());
            }
        }

        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                connection.unregister(this);
            }
        }
    }
}

