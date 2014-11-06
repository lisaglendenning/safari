package edu.uw.zookeeper.safari.backend;

import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.client.CreateOrEquals;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class BackendRequestService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}

        @Provides @Singleton
        public BackendRequestService getBackendRequestService(
                @Peer Identifier peer,
                @Backend ServerInetAddressView address,
                @Backend ZxidReference zxids,
                @Control Materializer<ControlZNode<?>,?> control,
                final Supplier<FutureTransition<RegionRoleService>> role,
                SchemaClientService<StorageZNode<?>,?> storage,
                BackendSessionExecutors sessions,
                ServerPeerConnections peers,
                ServiceMonitor monitor) throws Exception {
            BackendRequestService instance = BackendRequestService.create(
                    peer, address, zxids, control, storage, sessions, peers, 
                    ImmutableList.<Service.Listener>of(
                            new Service.Listener() {
                                
                                final EnumSet<EnsembleRole> ROLES = EnumSet.of(EnsembleRole.FOLLOWING, EnsembleRole.LEADING);
                                
                                @Override
                                public void starting() {
                                    FutureTransition<RegionRoleService> transition = role.get();
                                    RegionRoleService player = null;
                                    do {
                                        if (transition.getCurrent().isPresent()) {
                                            player =  transition.getCurrent().get();
                                        }
                                        if ((player == null) || !ROLES.contains(player.getRole().getRole())) {
                                            try {
                                                player = transition.getNext().get();
                                            } catch (Exception e) {
                                                throw Throwables.propagate(e);
                                            }
                                            transition = role.get();
                                        }
                                    } while (!ROLES.contains(player.getRole().getRole()));
                                }
                            }));
            monitor.add(instance);
            return instance;
        }

        @Override
        public Key<? extends Service> getKey() {
            return Key.get(BackendRequestService.class);
        }
        
        @Override
        protected void configure() {
        }
    }

    public static ListenableFuture<Optional<ServerInetAddressView>> advertise(
            final Identifier peer, 
            final ServerInetAddressView value, 
            final Materializer<ControlZNode<?>,?> materializer) {
        ZNodePath path = ControlSchema.Safari.Peers.Peer.StorageAddress.pathOf(peer);
        ListenableFuture<Optional<ServerInetAddressView>> task = CreateOrEquals.create(path, value, materializer);
        return Futures.transform(task, 
                new Function<Optional<ServerInetAddressView>, Optional<ServerInetAddressView>>() {
                    @Override
                    public Optional<ServerInetAddressView> apply(
                            Optional<ServerInetAddressView> input) {
                        if (input.isPresent()) {
                            throw new IllegalStateException(String.format("%s != %s", value, input.get()));
                        }
                        return input;
                    }
        });
    }
    
    public static BackendRequestService create(
            Identifier peer,
            ServerInetAddressView address,
            ZxidReference zxids,
            Materializer<ControlZNode<?>,?> control,
            SchemaClientService<StorageZNode<?>,?> storage,
            BackendSessionExecutors sessions,
            ServerPeerConnections peers,
            Iterable<? extends Service.Listener> listeners) {
        BackendRequestService instance = new BackendRequestService(
                sessions, 
                peers,
                zxids,
                ImmutableList.<Service.Listener>builder()
                .addAll(listeners)
                .add(new Advertiser(peer, address, control)).build());
        MessageXomegaSender.listen(
                peers, 
                storage.materializer().cache(), 
                storage.cacheEvents(), 
                instance, 
                instance.logger());
        return instance;
    }

    protected final BackendSessionExecutors sessions;
    protected final ConcurrentMap<Long, ServerPeerConnectionDispatcher.BackendSessionListener> listeners;
    protected final ZxidReference zxids;
    protected final ServerPeerConnectionsListener<ServerPeerConnectionDispatcher> listener;
    
    protected BackendRequestService(
            BackendSessionExecutors sessions,
            ServerPeerConnections peers,
            ZxidReference zxids,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.sessions = sessions;
        this.zxids = zxids;
        this.listeners = new MapMaker().makeMap();
        this.listener = new ServerPeerConnectionsListener<ServerPeerConnectionDispatcher>(new Function<ServerPeerConnection<?>, ServerPeerConnectionDispatcher>(){
            @Override
            public ServerPeerConnectionDispatcher apply(ServerPeerConnection<?> connection) {
                return new ServerPeerConnectionDispatcher(connection);
            }
        }, peers);
        addListener(listener, MoreExecutors.directExecutor());
    }

    protected static class Advertiser extends Service.Listener {

        protected final Identifier peer;
        protected final ServerInetAddressView value;
        protected final Materializer<ControlZNode<?>,?> control;
        
        public Advertiser(Identifier peer, ServerInetAddressView value, Materializer<ControlZNode<?>,?> control) {
            this.peer = peer;
            this.value = value;
            this.control = control;
        }
        
        @Override
        public void running() {
            try {
                advertise(peer, value, control).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    protected static class ServerPeerConnectionsListener<T extends Connection.Listener<? super MessagePacket>> extends Service.Listener implements ConnectionFactory.ConnectionsListener<ServerPeerConnection<?>>{
        
        protected final Function<ServerPeerConnection<?>, T> factory;
        protected final ServerPeerConnections connections;
        protected final ConcurrentMap<ServerPeerConnection<?>, T> listeners;
        
        public ServerPeerConnectionsListener(
                Function<ServerPeerConnection<?>, T> factory,
                ServerPeerConnections connections) {
            this.factory = factory;
            this.connections = connections;
            this.listeners = new MapMaker().weakValues().makeMap();
        }
        
        @Override
        public void starting() {
            connections.subscribe(this);
            for (ServerPeerConnection<?> c: connections) {
                handleConnectionOpen(c);
            }
        }
        
        @Override
        public void stopping(State from) {
            connections.unsubscribe(this);
        }

        @Override
        public void handleConnectionOpen(ServerPeerConnection<?> connection) {
            T listener = factory.apply(connection);
            if (listeners.putIfAbsent(connection, listener) == null) {
                connection.subscribe(listener);
            }
        }
    }
    
    @SuppressWarnings("rawtypes")
    protected class ServerPeerConnectionDispatcher implements Connection.Listener<MessagePacket>, FutureCallback<MessagePacket> {

        protected final ServerPeerConnection<?> connection;
        protected final Writer writer;
        protected final Set<Long> mine;
        
        public ServerPeerConnectionDispatcher(
                ServerPeerConnection<?> connection) {
            this.connection = connection;
            this.mine = Collections.synchronizedSet(Sets.<Long>newHashSet());
            this.writer = new Writer();
            connection.subscribe(this);
        }

        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            if (Connection.State.CONNECTION_CLOSED == state.to()) {
                onFailure(new ClosedChannelException());
            }
        }

        @Override
        public void handleConnectionRead(MessagePacket message) {
            logger.debug("{}", message);
            switch (message.getType()) {
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

        @Override
        public void onSuccess(MessagePacket message) {
            writer.submit(message);
        }

        @Override
        public void onFailure(Throwable t) {
            connection.unsubscribe(this);
            synchronized (mine) {
                for (Long session: Iterables.consumingIterable(mine)) {
                    BackendSessionListener listener = listeners.get(session);
                    if ((listener != null) && (listener.dispatcher() == this)) {
                        listener.stop();
                    }
                }
            }
        }
        
        protected void handleMessageSessionOpen(final MessageSessionOpenRequest message) {
            new SessionOpenListener(message, sessions.submit(message));

        }
        
        protected void handleMessageSessionRequest(MessageSessionRequest message) {
            final ShardedRequestMessage<?> request = (ShardedRequestMessage<?>) message.getMessage();
            try {
                BackendSessionListener listener = listeners.get(message.getIdentifier());
                if (listener != null) { 
                    if (listener.dispatcher() == this) {
                        listener.submit(request);
                    } else {
                        throw new KeeperException.SessionMovedException();
                    }
                } else {
                    throw new KeeperException.SessionExpiredException();
                }
            } catch (KeeperException e) {
                onSuccess(
                        MessagePacket.valueOf(
                                MessageSessionResponse.of(
                                        message.getIdentifier(), 
                                        ShardedServerResponseMessage.valueOf(
                                                request.getShard(), ProtocolResponseMessage.of(request.xid(), zxids.get(), new IErrorResponse(e.code()))))));
            }
        }
        
        protected class Writer implements FutureCallback<MessagePacket> {

            public Writer() {}
            
            public ListenableFuture<MessagePacket> submit(MessagePacket message) {
                ListenableFuture<MessagePacket> future = connection.write(message);
                Futures.addCallback(future, this);
                return future;
            }
            
            @Override
            public void onSuccess(MessagePacket result) {
            }

            @Override
            public void onFailure(Throwable t) {
                ServerPeerConnectionDispatcher.this.onFailure(t);
            }
            
        }
        
        protected class SessionOpenListener extends SimpleToStringListenableFuture<MessageSessionOpenResponse> implements Runnable {
            
            private final MessageSessionOpenRequest request;
            
            public SessionOpenListener(
                    MessageSessionOpenRequest request,
                    ListenableFuture<MessageSessionOpenResponse> future) {
                super(future);
                this.request = request;
                addListener(this, MoreExecutors.directExecutor());
            }

            @Override
            public void run() {
                if (isDone()) {
                    final Long session = request.getIdentifier();
                    MessageSessionOpenResponse response;
                    try {
                        response = get();
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    } catch (ExecutionException e) {
                        response = MessageSessionOpenResponse.of(
                                request.getIdentifier(), 
                                ConnectMessage.Response.Invalid.newInstance());
                    }
                    if (response.getMessage() instanceof ConnectMessage.Response.Valid) {
                        BackendSessionListener listener = listeners.get(session);
                        if ((listener == null) || (listener.dispatcher() != ServerPeerConnectionDispatcher.this)) {
                            BackendSessionExecutors.BackendSessionExecutor executor = sessions.get(request.getIdentifier());
                            if (executor != null) {
                                listener = new BackendSessionListener(executor);
                                BackendSessionListener existing = listeners.put(session, listener);
                                if (existing != null) {
                                    existing.stop();
                                }
                            }
                        }
                        mine.add(session);
                    }
                    onSuccess(MessagePacket.valueOf(response));
                }
            }
        }

        protected class BackendSessionListener implements FutureCallback<ShardedResponseMessage<?>>, SessionListener, TaskExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>> {

            protected final BackendSessionExecutors.BackendSessionExecutor executor;
            
            public BackendSessionListener(
                    BackendSessionExecutors.BackendSessionExecutor executor) {
                this.executor = executor;
                executor.client().subscribe(this);
            }
            
            public BackendSessionExecutors.BackendSessionExecutor executor() {
                return executor;
            }
            
            public ServerPeerConnectionDispatcher dispatcher() {
                return ServerPeerConnectionDispatcher.this;
            }
            
            public void stop() {
                executor().client().unsubscribe(this);
                if (listeners.remove(executor().session(), this)) {
                    mine.remove(executor().session());
                }
            }

            @Override
            public ListenableFuture<ShardedResponseMessage<?>> submit(ShardedRequestMessage<?> request) {
                ListenableFuture<ShardedResponseMessage<?>> future =
                        ToErrorMessage.submit(
                                executor().client(), 
                                request);
                Futures.addCallback(future, this);
                return future;
            }

            @Override
            public void onSuccess(ShardedResponseMessage<?> response) {
                dispatcher().onSuccess(MessagePacket.valueOf(MessageSessionResponse.of(
                        executor().session(), response)));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("{}", this, t);
                stop();
                executor().client().stop();
            }

            @Override
            public void handleAutomatonTransition(
                    Automaton.Transition<ProtocolState> transition) {
                switch (transition.to()) {
                case DISCONNECTED:
                    executor().client().unsubscribe(this);
                    break;
                case ERROR:
                    onFailure(new KeeperException.ConnectionLossException());
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
    }
}
