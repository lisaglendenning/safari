package edu.uw.zookeeper.safari.peer;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ForwardingServiceListener;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.ServiceMonitor;
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
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.FramedMessagePacketCodec;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;

public class PeerConnectionsService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

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
        
        
        protected Module() {}

        @Provides @Singleton
        public PeerConnectionsService getPeerConnectionsService(
                @Peer Identifier id,
                @Peer TimeValue timeOut,
                ServerPeerConnections servers,
                ClientPeerConnections clients,
                ObjectMapper mapper,
                ScheduledExecutorService scheduler,
                Executor executor,
                ControlClientService control,
                ServiceMonitor monitor,
                Factory<InetSocketAddress> addresses) throws InterruptedException, ExecutionException, KeeperException {
            PeerConnectionsService instance = PeerConnectionsService.create(
                    id,
                    timeOut,
                    servers,
                    clients,
                    executor,
                    scheduler,
                    addresses,
                    control.materializer());
            monitor.add(instance);
            return instance;
        }

        @SuppressWarnings("rawtypes")
        @Provides @Singleton
        public ClientPeerConnections getClientPeerConnections(
                @Peer Identifier id,
                @Peer TimeValue timeOut,
                ServiceMonitor monitor,
                ScheduledExecutorService scheduler,
                ObjectMapper mapper,
                NetClientModule clients,
                ControlClientService control) {
            ClientConnectionFactory<? extends Connection<MessagePacket,MessagePacket,?>> clientConnections =  
                    clients.getClientConnectionFactory(
                            codecFactory(mapper), 
                            connectionFactory()).get();
            ClientPeerConnections instance = ClientPeerConnections.newInstance(id, timeOut, scheduler, peerAddressLookup(control.materializer()), clientConnections);
            monitor.add(instance);
            return instance;
        }

        @SuppressWarnings("rawtypes")
        @Provides @Singleton
        public ServerPeerConnections getServerPeerConnections(
                @Peer Identifier id,
                @Peer TimeValue timeOut,
                @Peer ServerInetAddressView address,
                ServiceMonitor monitor,
                ScheduledExecutorService scheduler,
                ObjectMapper mapper,
                NetServerModule servers) {
            ServerConnectionFactory<? extends Connection<MessagePacket,MessagePacket,?>> serverConnections = 
                    servers.getServerConnectionFactory(
                            codecFactory(mapper), 
                            connectionFactory())
                    .get(address.get());
            ServerPeerConnections instance = ServerPeerConnections.newInstance(id, timeOut, scheduler, serverConnections);
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {}
    }
    
    public static ListenableFuture<Void> advertise(
            final Identifier peer, 
            final Materializer<ControlZNode<?>,?> materializer) {
        ZNodePath path = ControlSchema.Safari.Peers.PATH.join(ZNodeLabel.fromString(peer.toString())).join(ControlSchema.Safari.Peers.Peer.Presence.LABEL);
        return Futures.transform(materializer.create(path).call(), 
                new Function<Operation.ProtocolResponse<?>, Void>() {
                    @Override
                    public Void apply(
                            Operation.ProtocolResponse<?> input) {
                        try {
                            Operations.unlessError(input.record());
                        } catch (KeeperException e) {
                            throw new IllegalStateException(String.format("Error creating presence for %s", peer), e);
                        }
                        return null;
                    }
        });
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
    
    public static PeerConnectionsService create(
            final Identifier id,
            final TimeValue timeOut,
            final ServerPeerConnections servers,
            final ClientPeerConnections clients,
            final Executor executor,
            final ScheduledExecutorService scheduler,
            final Factory<InetSocketAddress> addresses,
            final Materializer<ControlZNode<?>,?> control) {
        PeerConnectionsService instance = new PeerConnectionsService(
                ImmutableList.of(
                        ForwardingServiceListener.forService(servers),
                        ForwardingServiceListener.forService(clients),
                        new ConnectToSelf(id, timeOut, servers, clients, executor, scheduler, addresses),
                        new Advertiser(id, control)));
        return instance;
    }
    
    protected PeerConnectionsService(
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
    }
    
    protected static class ConnectToSelf extends Service.Listener {

        protected final Identifier id;
        protected final TimeValue timeOut;
        protected final ServerPeerConnections servers;
        protected final ClientPeerConnections clients;
        protected final Executor executor;
        protected final ScheduledExecutorService scheduler;
        protected final Factory<InetSocketAddress> addresses;
        
        public ConnectToSelf(
                Identifier id,
                TimeValue timeOut,
                ServerPeerConnections servers,
                ClientPeerConnections clients,
                Executor executor,
                ScheduledExecutorService scheduler,
                Factory<InetSocketAddress> addresses) {
            this.id = id;
            this.timeOut = timeOut;
            this.scheduler = scheduler;
            this.servers = servers;
            this.clients = clients;
            this.addresses = addresses;
            this.executor = executor;
        }
        
        @SuppressWarnings("rawtypes")
        @Override
        public void starting() {
            IntraVmEndpointFactory<MessagePacket, MessagePacket> endpoints = IntraVmEndpointFactory.create(
                    addresses,  
                    IntraVmEndpointFactory.actorExecutors(executor));
            IntraVmEndpoint<MessagePacket, MessagePacket> serverLoopback = endpoints.get();
            IntraVmEndpoint<MessagePacket, MessagePacket> clientLoopback = endpoints.get();
            Connection<? super MessagePacket, ? extends MessagePacket, ?> loopbackServer = IntraVmConnection.newInstance(serverLoopback, clientLoopback);
            Connection<? super MessagePacket, ? extends MessagePacket, ?> loopbackClient = IntraVmConnection.newInstance(clientLoopback, serverLoopback);
            servers.put(ServerPeerConnection.<Connection<? super MessagePacket, ? extends MessagePacket, ?>>create(id, id, loopbackServer, timeOut, scheduler));
            clients.put(ClientPeerConnection.<Connection<? super MessagePacket, ? extends MessagePacket, ?>>create(id, id, loopbackClient, timeOut, scheduler));
        }
    }
    
    protected static class Advertiser extends Service.Listener {

        protected final Identifier id;
        protected final Materializer<ControlZNode<?>,?> control;
        
        public Advertiser(Identifier id, Materializer<ControlZNode<?>,?> control) {
            this.id = id;
            this.control = control;
        }
        
        @Override
        public void running() {
            try {
                advertise(id, control).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}

