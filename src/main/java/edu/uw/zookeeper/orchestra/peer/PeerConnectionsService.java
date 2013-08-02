package edu.uw.zookeeper.orchestra.peer;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;


import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmConnectionEndpoint;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ServerPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.FramedMessagePacketCodec;
import edu.uw.zookeeper.orchestra.peer.protocol.JacksonModule;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacketCodec;

public class PeerConnectionsService<T> extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

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
        
        @Override
        protected void configure() {
            TypeLiteral<PeerConnectionsService<?>> generic = new TypeLiteral<PeerConnectionsService<?>>() {};
            bind(PeerConnectionsService.class).to(generic);
            bind(generic).to(new TypeLiteral<PeerConnectionsService<Connection<MessagePacket>>>() {});
        }

        @Provides @Singleton
        public PeerConnectionsService<Connection<MessagePacket>> getPeerConnectionsService(
                PeerConfiguration configuration,
                ControlMaterializerService<?> control,
                ServiceLocator locator,
                NettyModule netModule,
                ServiceMonitor monitor,
                Factory<Publisher> publishers) throws InterruptedException, ExecutionException, KeeperException {
            ServerConnectionFactory<Connection<MessagePacket>> serverConnections = 
                    netModule.servers().get(
                            codecFactory(JacksonModule.getMapper()), 
                            connectionFactory())
                    .get(configuration.getView().address().get());
            monitor.addOnStart(serverConnections);
            ClientConnectionFactory<Connection<MessagePacket>> clientConnections =  
                    netModule.clients().get(
                            codecFactory(JacksonModule.getMapper()), 
                            connectionFactory()).get();
            monitor.addOnStart(clientConnections);
            Pair<IntraVmConnectionEndpoint<InetSocketAddress>, IntraVmConnectionEndpoint<InetSocketAddress>> loopbackEndpoints = Pair.create(
                    IntraVmConnectionEndpoint.create(InetSocketAddress.createUnresolved("localhost", 1), publishers.get(), MoreExecutors.sameThreadExecutor()),
                    IntraVmConnectionEndpoint.create(InetSocketAddress.createUnresolved("localhost", 2), publishers.get(), MoreExecutors.sameThreadExecutor()));
            Pair<IntraVmConnection<InetSocketAddress>, IntraVmConnection<InetSocketAddress>> loopback = 
                    IntraVmConnection.createPair(loopbackEndpoints);
            PeerConnectionsService<Connection<MessagePacket>> instance = 
                    PeerConnectionsService.newInstance(
                            configuration.getView().id(), 
                            serverConnections, 
                            clientConnections, 
                            loopback, 
                            control.materializer(),
                            locator);
            monitor.addOnStart(instance);
            return instance;
        }
    }
    
    public static <C extends Connection<? super MessagePacket>> PeerConnectionsService<C> newInstance(
            Identifier identifier,
            ServerConnectionFactory<C> serverConnectionFactory,
            ClientConnectionFactory<C> clientConnectionFactory,
            Pair<? extends Connection<? super MessagePacket>, ? extends Connection<? super MessagePacket>> loopback,
            Materializer<?,?> control,
            ServiceLocator locator) {
        PeerConnectionsService<C> instance = new PeerConnectionsService<C>(
                identifier, 
                serverConnectionFactory, 
                clientConnectionFactory, 
                loopback, 
                control);
        instance.new Advertiser(locator, MoreExecutors.sameThreadExecutor());
        return instance;
    }

    protected final Identifier identifier;
    protected final ServerPeerConnections<?> servers;
    protected final ClientPeerConnections<?> clients;
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected PeerConnectionsService(
            Identifier identifier,
            ServerConnectionFactory<?> serverConnectionFactory,
            ClientConnectionFactory<?> clientConnectionFactory,
            Pair<? extends Connection<? super MessagePacket>, ? extends Connection<? super MessagePacket>> loopback,
            Materializer<?,?> control) {
        this.identifier = identifier;
        this.servers = new ServerPeerConnections(identifier, serverConnectionFactory);
        this.clients = new ClientPeerConnections(identifier, control, clientConnectionFactory);
        
        servers.put(new ServerPeerConnection(identifier, identifier, loopback.first()));
        clients.put(new ClientPeerConnection(identifier, identifier, loopback.second()));
    }
    
    public Identifier identifier() {
        return identifier;
    }
    
    public ClientPeerConnections<?> clients() {
        return clients;
    }

    public ServerPeerConnections<?> servers() {
        return servers;
    }
    
    @Override
    protected void startUp() throws Exception {
        servers().start().get();
        clients().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        servers().stop().get();
        clients().stop().get();
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
            Materializer<?,?> materializer = locator.getInstance(ControlMaterializerService.class).materializer();
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

