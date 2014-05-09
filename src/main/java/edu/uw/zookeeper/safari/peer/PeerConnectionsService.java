package edu.uw.zookeeper.safari.peer;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.SameThreadExecutor;
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
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
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
        
        public static CachedFunction<Identifier, ServerInetAddressView> peerAddressLookup(
            final Materializer<ControlZNode<?>,?> materializer) {
            final Function<Identifier, ZNodePath> toPath = new Function<Identifier, ZNodePath>() {
                @Override
                public ZNodePath apply(Identifier peer) {
                    return ControlSchema.Safari.Peers.PATH.join(ZNodeLabel.fromString(peer.toString())).join(ControlSchema.Safari.Peers.Peer.PeerAddress.LABEL);
                }
            };
            final Function<Identifier, ServerInetAddressView> cached = new Function<Identifier, ServerInetAddressView>() {
                @Override
                public @Nullable ServerInetAddressView apply(
                        final Identifier peer) {
                    ServerInetAddressView address = null;
                    materializer.cache().lock().readLock().lock();
                    try {
                        ControlSchema.Safari.Peers.Peer.PeerAddress node =
                                (ControlSchema.Safari.Peers.Peer.PeerAddress) materializer.cache().cache().get(toPath.apply(peer));
                        if (node != null) {
                            address = node.data().get();
                        }
                    } finally {
                        materializer.cache().lock().readLock().unlock();
                    }
                    return address;
                }
            };
            final AsyncFunction<Identifier, ServerInetAddressView> lookup = new AsyncFunction<Identifier, ServerInetAddressView>() {
                @Override
                public ListenableFuture<ServerInetAddressView> apply(final Identifier peer) {
                    final ZNodePath path = toPath.apply(peer);
                    return Futures.transform(materializer.getData(path).call(),
                            new Function<Operation.ProtocolResponse<?>, ServerInetAddressView>() {
                                @Override
                                public @Nullable ServerInetAddressView apply(Operation.ProtocolResponse<?> input) {
                                    try {
                                        Operations.unlessError(input.record());
                                    } catch (KeeperException e) {
                                        return null;
                                    }
                                    return cached.apply(peer);
                                }
                            });
                }
            };
            return CachedFunction.create(cached, lookup, LogManager.getLogger(PeerConnectionsService.class));
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
            Materializer<ControlZNode<?>,?> control,
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
        instance.new Advertiser(SameThreadExecutor.getInstance());
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
            Materializer<ControlZNode<?>,?> control,
            Injector injector) {
        super(injector);
        this.logger = LogManager.getLogger(getClass());
        this.identifier = identifier;
        this.servers = ServerPeerConnections.newInstance(identifier, timeOut, executor, serverConnectionFactory);
        this.clients = ClientPeerConnections.newInstance(identifier, timeOut, executor, Module.peerAddressLookup(control), clientConnectionFactory);
        
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
            Materializer<ControlZNode<?>,?> materializer = injector().getInstance(ControlMaterializerService.class).materializer();
            try {
                PeerConfiguration.advertise(identifier(), materializer).get();
            } catch (Exception e) {
                logger.warn("", e);
                injector.getInstance(PeerConnectionsService.class).stopAsync();
            }
        }
    }
}

