package edu.uw.zookeeper.safari.peer;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;
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
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;

@DependsOn({ServerPeerConnections.class, ClientPeerConnections.class})
public class PeerConnectionsService extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        @SuppressWarnings("rawtypes")
        public static Factory<Codec<MessagePacket,MessagePacket,MessagePacket,MessagePacket>> codecFactory(
                final ObjectMapper mapper) {
            return new Factory<Codec<MessagePacket,MessagePacket,MessagePacket,MessagePacket>>() {
                @Override
                public FramedMessagePacketCodec get() {
                    return FramedMessagePacketCodec.defaults(mapper);
                }
            };
        }
        
        @SuppressWarnings("rawtypes")
        public static ParameterizedFactory<CodecConnection<MessagePacket,MessagePacket,Codec<MessagePacket,MessagePacket,MessagePacket,MessagePacket>,?>, Connection<MessagePacket,MessagePacket,?>> connectionFactory() {
            return new ParameterizedFactory<CodecConnection<MessagePacket,MessagePacket,Codec<MessagePacket,MessagePacket,MessagePacket,MessagePacket>,?>, Connection<MessagePacket,MessagePacket,?>>() {
                @Override
                public Connection<MessagePacket,MessagePacket,?> get(
                        CodecConnection<MessagePacket,MessagePacket,Codec<MessagePacket,MessagePacket,MessagePacket,MessagePacket>,?> value) {
                    return value;
                }
            };
        }
        
        public Module() {}

        @SuppressWarnings("rawtypes")
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
            ServerConnectionFactory<? extends Connection<MessagePacket,MessagePacket,?>> serverConnections = 
                    servers.getServerConnectionFactory(
                            codecFactory(mapper), 
                            connectionFactory())
                    .get(configuration.getView().address().get());
            ClientConnectionFactory<? extends Connection<MessagePacket,MessagePacket,?>> clientConnections =  
                    clients.getClientConnectionFactory(
                            codecFactory(mapper), 
                            connectionFactory()).get();
            IntraVmEndpointFactory<MessagePacket, MessagePacket> endpoints = IntraVmEndpointFactory.create(
                    addresses,  
                    IntraVmEndpointFactory.actorExecutors(executor));
            IntraVmEndpoint<MessagePacket, MessagePacket> serverLoopback = endpoints.get();
            IntraVmEndpoint<MessagePacket, MessagePacket> clientLoopback = endpoints.get();
            PeerConnectionsService instance = PeerConnectionsService.newInstance(
                    configuration.getView().id(), 
                    configuration.getTimeOut(),
                    scheduler,
                    serverConnections, 
                    clientConnections,
                    IntraVmConnection.newInstance(serverLoopback, clientLoopback),
                    IntraVmConnection.newInstance(clientLoopback, serverLoopback),
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
    
    @SuppressWarnings("rawtypes")
    public static PeerConnectionsService newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> serverConnectionFactory,
            ClientConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> clientConnectionFactory,
            Connection<? super MessagePacket, ? extends MessagePacket, ?> loopbackServer,
            Connection<? super MessagePacket, ? extends MessagePacket, ?> loopbackClient,
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

    protected final Logger logger;
    protected final Identifier identifier;
    protected final ServerPeerConnections servers;
    protected final ClientPeerConnections clients;
    
    @SuppressWarnings("rawtypes")
    protected PeerConnectionsService(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            ServerConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> serverConnectionFactory,
            ClientConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> clientConnectionFactory,
            Connection<? super MessagePacket, ? extends MessagePacket, ?> loopbackServer,
            Connection<? super MessagePacket, ? extends MessagePacket, ?> loopbackClient,
            Materializer<?> control,
            Injector injector) {
        super(injector);
        this.logger = LogManager.getLogger(getClass());
        this.identifier = identifier;
        this.servers = ServerPeerConnections.newInstance(identifier, timeOut, executor, serverConnectionFactory);
        this.clients = ClientPeerConnections.newInstance(identifier, timeOut, executor, ControlSchema.Peers.Entity.PeerAddress.lookup(control), clientConnectionFactory);
        
        servers.put(ServerPeerConnection.<Connection<? super MessagePacket, ? extends MessagePacket, ?>>create(identifier, identifier, loopbackServer, timeOut, executor));
        clients.put(ClientPeerConnection.<Connection<? super MessagePacket, ? extends MessagePacket, ?>>create(identifier, identifier, loopbackClient, timeOut, executor));
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
                logger.warn("", e);
                injector.getInstance(PeerConnectionsService.class).stopAsync();
            }
        }
    }
}

