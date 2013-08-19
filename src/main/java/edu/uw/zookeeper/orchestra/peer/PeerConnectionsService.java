package edu.uw.zookeeper.orchestra.peer;

import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmEndpoint;
import edu.uw.zookeeper.net.intravm.IntraVmEndpointFactory;
import edu.uw.zookeeper.orchestra.DependentModule;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ServerPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.FramedMessagePacketCodec;
import edu.uw.zookeeper.orchestra.peer.protocol.JacksonModule;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacketCodec;

@DependsOn({ServerPeerConnections.class, ClientPeerConnections.class})
public class PeerConnectionsService extends DependentService.SimpleDependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

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
        
        public Module() {}

        @Provides @Singleton
        public PeerConnectionsService getPeerConnectionsService(
                PeerConfiguration configuration,
                ControlMaterializerService<?> control,
                ServiceLocator locator,
                NetServerModule servers,
                NetClientModule clients,
                Factory<? extends SocketAddress> addresses,
                Factory<? extends Publisher> publishers,
                DependentServiceMonitor monitor) throws InterruptedException, ExecutionException, KeeperException {
            ServerConnectionFactory<Connection<MessagePacket>> serverConnections = 
                    servers.getServerConnectionFactory(
                            codecFactory(JacksonModule.getMapper()), 
                            connectionFactory())
                    .get(configuration.getView().address().get());
            ClientConnectionFactory<Connection<MessagePacket>> clientConnections =  
                    clients.getClientConnectionFactory(
                            codecFactory(JacksonModule.getMapper()), 
                            connectionFactory()).get();
            IntraVmEndpointFactory<MessagePacket> endpoints = IntraVmEndpointFactory.create(
                    addresses, publishers, IntraVmEndpointFactory.sameThreadExecutors());
            IntraVmEndpoint<MessagePacket> serverLoopback = endpoints.get();
            IntraVmEndpoint<MessagePacket> clientLoopback = endpoints.get();
            PeerConnectionsService instance = monitor.listen(PeerConnectionsService.newInstance(
                    configuration.getView().id(), 
                    serverConnections, 
                    clientConnections,
                    IntraVmConnection.create(serverLoopback, clientLoopback),
                    IntraVmConnection.create(clientLoopback, serverLoopback),
                    control.materializer(),
                    locator));
            return instance;
        }

        @Provides @Singleton
        public ClientPeerConnections getClientPeerConnections(
                PeerConnectionsService service) {
            return service.clients();
        }

        @Provides @Singleton
        public ServerPeerConnections getServerPeerConnections(
                PeerConnectionsService service) {
            return service.servers();
        }

        @Override
        protected com.google.inject.Module[] getModules() {
            com.google.inject.Module[] modules = { PeerConfiguration.module() };
            return modules;
        }
    }
    
    public static PeerConnectionsService newInstance(
            Identifier identifier,
            ServerConnectionFactory<? extends Connection<? super MessagePacket>> serverConnectionFactory,
            ClientConnectionFactory<? extends Connection<? super MessagePacket>> clientConnectionFactory,
            Connection<? super MessagePacket> loopbackServer,
            Connection<? super MessagePacket> loopbackClient,
            Materializer<?> control,
            ServiceLocator locator) {
        PeerConnectionsService instance = new PeerConnectionsService(
                identifier, 
                serverConnectionFactory, 
                clientConnectionFactory, 
                loopbackServer,
                loopbackClient,
                control,
                locator);
        instance.new Advertiser(locator, MoreExecutors.sameThreadExecutor());
        return instance;
    }

    protected final Identifier identifier;
    protected final ServerPeerConnections servers;
    protected final ClientPeerConnections clients;
    
    protected PeerConnectionsService(
            Identifier identifier,
            ServerConnectionFactory<? extends Connection<? super MessagePacket>> serverConnectionFactory,
            ClientConnectionFactory<? extends Connection<? super MessagePacket>> clientConnectionFactory,
            Connection<? super MessagePacket> loopbackServer,
            Connection<? super MessagePacket> loopbackClient,
            Materializer<?> control,
            ServiceLocator locator) {
        super(locator);
        this.identifier = identifier;
        this.servers = new ServerPeerConnections(identifier, serverConnectionFactory);
        this.clients = new ClientPeerConnections(identifier, ControlSchema.Peers.Entity.PeerAddress.lookup(control), clientConnectionFactory);
        
        servers.put(ServerPeerConnection.<Connection<? super MessagePacket>>create(identifier, identifier, loopbackServer));
        clients.put(ClientPeerConnection.<Connection<? super MessagePacket>>create(identifier, identifier, loopbackClient));
    }
    
    public Identifier identifier() {
        return identifier;
    }
    
    public ClientPeerConnections clients() {
        return clients;
    }

    public ServerPeerConnections servers() {
        return servers;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected void shutDown() throws Exception {
        Futures.allAsList(servers().stop(), clients().stop()).get();
        
        super.shutDown();
    }

    public class Advertiser implements Service.Listener {
    
        protected final ServiceLocator locator;
        
        public Advertiser(
                ServiceLocator locator,
                Executor executor) {
            this.locator = locator;
            addListener(this, executor);
        }
        
        @Override
        public void starting() {
        }
    
        @Override
        public void running() {
            Materializer<?> materializer = locator.getInstance(ControlMaterializerService.class).materializer();
            try {
                PeerConfiguration.advertise(identifier(), materializer);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    
        @Override
        public void stopping(State from) {
        }
    
        @Override
        public void terminated(State from) {
        }
    
        @Override
        public void failed(State from, Throwable failure) {
        }
    }
}

