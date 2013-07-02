package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.protocol.FramedMessagePacketCodec;
import edu.uw.zookeeper.orchestra.protocol.MessageHandshake;
import edu.uw.zookeeper.orchestra.protocol.JacksonModule;
import edu.uw.zookeeper.orchestra.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.protocol.MessagePacketCodec;
import edu.uw.zookeeper.orchestra.protocol.MessageType;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;

public class ConductorPeerService extends AbstractIdleService {

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
        public ConductorPeerService getConductorPeerService(
                ConductorConfiguration configuration,
                ServiceLocator locator,
                NettyModule netModule,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            ServerConnectionFactory<MessagePacket, Connection<MessagePacket>> serverConnections = 
                    netModule.servers().get(
                            codecFactory(JacksonModule.getMapper()), 
                            connectionFactory())
                    .get(configuration.get().address().get());
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
            ConductorPeerService instance = new ConductorPeerService(configuration.get().id(), serverConnections, clientConnections, loopback, locator);
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
    
    public ConductorPeerService(
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
    
    public ListenableFuture<ClientPeerConnection> connect(Identifier identifier) {
        return new ConnectTask(identifier);
    }
    
    @Override
    protected void startUp() throws Exception {
        new OnConnectListener();
        
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
                second().unregister(this);
            }
        }
    }

    public class ClientPeerConnection extends PeerConnection {
    
        public ClientPeerConnection(Identifier first, Connection<? super MessagePacket> second) {
            super(first, second);
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
        }

        @Override
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            super.handleTransition(event);
            
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                serverConnections.remove(first(), this);
            }
        }
    }
    
    protected class ConnectTask extends PromiseTask<Identifier, ClientPeerConnection> implements FutureCallback<Connection<MessagePacket>> {

        protected ConnectTask(Identifier task) {
            this(task, PromiseTask.<ClientPeerConnection>newPromise());
        }
        
        protected ConnectTask(Identifier task,
                Promise<ClientPeerConnection> delegate) {
            super(task, delegate);
            
            Materializer materializer = locator.getInstance(ControlClientService.class).materializer();
            try {
                Orchestra.Conductors.Entity.ConductorAddress address = Orchestra.Conductors.Entity.ConductorAddress.lookup(Orchestra.Conductors.Entity.of(task), materializer);
                Futures.addCallback(clientConnectionFactory.connect(address.get().get()), this);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onSuccess(Connection<MessagePacket> result) {
            ClientPeerConnection peered = new ClientPeerConnection(task(), result);
            ClientPeerConnection prev = clientConnections.putIfAbsent(peered.first(), peered);
            if (prev != null) {
                result.close();
                peered = prev;
            } else {
                peered.second().write(MessagePacket.of(MessageHandshake.of(identifier)));
            }
            set(peered);
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }

    protected class OnConnectListener {
        protected OnConnectListener() {
            serverConnectionFactory.register(this);
        }

        @Subscribe
        public void handleServerConnection(Connection<MessagePacket> connection) {
            new OnConnectTask(connection);
        }
    }
        
    protected class OnConnectTask {

        protected final Connection<MessagePacket> connection;
        
        protected OnConnectTask(Connection<MessagePacket> connection) {
            this.connection = connection;
            
            connection.register(this);
        }
        
        @Subscribe
        public void handleMessage(MessagePacket event) {
            if (MessageType.MESSAGE_TYPE_HANDSHAKE == event.first().type()) {
                MessageHandshake body = (MessageHandshake) event.second();
                ServerPeerConnection peered = new ServerPeerConnection(body.getId(), connection);
                ServerPeerConnection prev = serverConnections.putIfAbsent(peered.first(), peered);
                if (prev != null) {
                    prev.second().close();
                    serverConnections.remove(prev.first(), prev);
                    prev = serverConnections.putIfAbsent(peered.first(), peered);
                    if (prev != null) {
                        throw new IllegalStateException();
                    }
                }
                connection.unregister(this);
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

