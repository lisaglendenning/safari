package edu.uw.zookeeper.safari.peer;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.net.intravm.IntraVmConnection;
import edu.uw.zookeeper.net.intravm.IntraVmEndpoint;
import edu.uw.zookeeper.net.intravm.IntraVmEndpointFactory;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.FramedMessagePacketCodec;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacketCodec;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;

@DependsOn({ServerPeerConnections.class, ClientPeerConnections.class})
public class PeerConnectionsService extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public static ParameterizedFactory<Publisher, Pair<Class<MessagePacket<?>>, FramedMessagePacketCodec>> codecFactory(
                final ObjectMapper mapper) {
            return new ParameterizedFactory<Publisher, Pair<Class<MessagePacket<?>>, FramedMessagePacketCodec>>() {
                
                @SuppressWarnings("unchecked")
                protected final Pair<Class<MessagePacket<?>>, FramedMessagePacketCodec> codec = Pair.create((Class<MessagePacket<?>>) (Class<?>) MessagePacket.class, FramedMessagePacketCodec.newInstance(MessagePacketCodec.newInstance(mapper)));
                
                @Override
                public Pair<Class<MessagePacket<?>>, FramedMessagePacketCodec> get(
                        Publisher value) {
                    return codec;
                }
            };
        }
        
        public static ParameterizedFactory<Pair<? extends Pair<Class<MessagePacket<?>>, ? extends FramedMessagePacketCodec>, Connection<MessagePacket<?>>>, Connection<MessagePacket<?>>> connectionFactory() {
            return new ParameterizedFactory<Pair<? extends Pair<Class<MessagePacket<?>>, ? extends FramedMessagePacketCodec>, Connection<MessagePacket<?>>>, Connection<MessagePacket<?>>>() {
                @Override
                public Connection<MessagePacket<?>> get(
                        Pair<? extends Pair<Class<MessagePacket<?>>, ? extends FramedMessagePacketCodec>, Connection<MessagePacket<?>>> value) {
                    return value.second();
                }
            };
        }
        
        public Module() {}

        @Provides @Singleton
        public PeerConnectionsService getPeerConnectionsService(
                ObjectMapper mapper,
                PeerConfiguration configuration,
                ScheduledExecutorService scheduler,
                Executor executor,
                ControlMaterializerService control,
                Injector injector,
                NetServerModule servers,
                NetClientModule clients,
                Factory<? extends SocketAddress> addresses) throws InterruptedException, ExecutionException, KeeperException {
            ServerConnectionFactory<Connection<MessagePacket<?>>> serverConnections = 
                    servers.getServerConnectionFactory(
                            codecFactory(mapper), 
                            connectionFactory())
                    .get(configuration.getView().address().get());
            ClientConnectionFactory<Connection<MessagePacket<?>>> clientConnections =  
                    clients.getClientConnectionFactory(
                            codecFactory(mapper), 
                            connectionFactory()).get();
            IntraVmEndpointFactory<MessagePacket<?>> endpoints = IntraVmEndpointFactory.create(
                    addresses, 
                    EventBusPublisher.factory(), 
                    IntraVmEndpointFactory.actorExecutors(executor));
            IntraVmEndpoint<MessagePacket<?>> serverLoopback = endpoints.get();
            IntraVmEndpoint<MessagePacket<?>> clientLoopback = endpoints.get();
            PeerConnectionsService instance = PeerConnectionsService.newInstance(
                    configuration.getView().id(), 
                    configuration.getTimeOut(),
                    scheduler,
                    serverConnections, 
                    clientConnections,
                    IntraVmConnection.create(serverLoopback, clientLoopback),
                    IntraVmConnection.create(clientLoopback, serverLoopback),
                    control.materializer(),
                    injector);
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
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(
                    PeerConfiguration.module());
        }
    }
    
    public static PeerConnectionsService newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket<?>>> serverConnectionFactory,
            ClientConnectionFactory<? extends Connection<? super MessagePacket<?>>> clientConnectionFactory,
            Connection<? super MessagePacket<?>> loopbackServer,
            Connection<? super MessagePacket<?>> loopbackClient,
            Materializer<?> control,
            Injector injector) {
        PeerConnectionsService instance = new PeerConnectionsService(
                identifier, 
                timeOut,
                executor,
                serverConnectionFactory, 
                clientConnectionFactory, 
                loopbackServer,
                loopbackClient,
                control,
                injector);
        instance.new Advertiser(MoreExecutors.sameThreadExecutor());
        return instance;
    }

    protected final Identifier identifier;
    protected final ServerPeerConnections servers;
    protected final ClientPeerConnections clients;
    
    protected PeerConnectionsService(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket<?>>> serverConnectionFactory,
            ClientConnectionFactory<? extends Connection<? super MessagePacket<?>>> clientConnectionFactory,
            Connection<? super MessagePacket<?>> loopbackServer,
            Connection<? super MessagePacket<?>> loopbackClient,
            Materializer<?> control,
            Injector injector) {
        super(injector);
        this.identifier = identifier;
        this.servers = new ServerPeerConnections(identifier, timeOut, executor, serverConnectionFactory);
        this.clients = new ClientPeerConnections(identifier, timeOut, executor, ControlSchema.Peers.Entity.PeerAddress.lookup(control), clientConnectionFactory);
        
        servers.put(ServerPeerConnection.<Connection<? super MessagePacket<?>>>create(identifier, identifier, loopbackServer, timeOut, executor));
        clients.put(ClientPeerConnection.<Connection<? super MessagePacket<?>>>create(identifier, identifier, loopbackClient, timeOut, executor));
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

    public class Advertiser extends Service.Listener {
    
        public Advertiser(
                Executor executor) {
            addListener(this, executor);
        }
    
        @Override
        public void running() {
            Materializer<?> materializer = injector().getInstance(ControlMaterializerService.class).materializer();
            try {
                PeerConfiguration.advertise(identifier(), materializer);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}

