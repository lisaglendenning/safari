package edu.uw.zookeeper.safari.backend;

import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ClientSession;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.CreateOrEquals;
import edu.uw.zookeeper.safari.peer.Peer;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.ShardedClientRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;

public class BackendRequestService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        protected Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public BackendRequestService getBackendRequestService(
                @Peer Identifier peer,
                @Backend ServerInetAddressView address,
                @Backend ZxidReference zxids,
                @Control Materializer<ControlZNode<?>,?> control,
                AsyncFunction<MessageSessionOpenRequest, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> clientFactory,
                ServerPeerConnections peers,
                ServiceMonitor monitor) throws Exception {
            BackendRequestService instance = BackendRequestService.newInstance(
                    peer, address, zxids, control, clientFactory, peers, ImmutableList.<Service.Listener>of());
            monitor.add(instance);
            return instance;
        }
    }

    public static ListenableFuture<Optional<ServerInetAddressView>> advertise(
            final Identifier peer, 
            final ServerInetAddressView value, 
            final Materializer<ControlZNode<?>,?> materializer) {
        ZNodePath path = ControlSchema.Safari.Peers.Peer.StorageAddress.pathOf(peer);
        return Futures.transform(CreateOrEquals.create(path, value, materializer), 
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
    
    public static BackendRequestService newInstance(
            Identifier peer,
            ServerInetAddressView address,
            ZxidReference zxids,
            Materializer<ControlZNode<?>,?> control,
            AsyncFunction<MessageSessionOpenRequest, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> clientFactory,
            ServerPeerConnections peers,
            Iterable<? extends Service.Listener> listeners) {
        BackendRequestService instance = new BackendRequestService(
                clientFactory, 
                peers,
                zxids,
                ImmutableList.<Service.Listener>builder()
                .addAll(listeners)
                .add(new Advertiser(peer, address, control)).build());
        return instance;
    }

    protected final AsyncFunction<MessageSessionOpenRequest, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> clientFactory;
    protected final ConcurrentMap<Long, ServerPeerConnectionDispatcher.BackendSessionListener> sessions;
    protected final ZxidReference zxids;
    protected final ServerPeerConnectionsListener<ServerPeerConnectionDispatcher> listener;
    
    protected BackendRequestService(
            AsyncFunction<MessageSessionOpenRequest, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> clientFactory,
            ServerPeerConnections peers,
            ZxidReference zxids,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.clientFactory = clientFactory;
        this.zxids = zxids;
        this.sessions = new MapMaker().makeMap();
        this.listener = new ServerPeerConnectionsListener<ServerPeerConnectionDispatcher>(new Function<ServerPeerConnection<?>, ServerPeerConnectionDispatcher>(){
            @Override
            public ServerPeerConnectionDispatcher apply(ServerPeerConnection<?> connection) {
                return new ServerPeerConnectionDispatcher(connection);
            }
        }, peers);
        addListener(listener, SameThreadExecutor.getInstance());
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

        @Override
        public void onSuccess(MessagePacket message) {
            writer.submit(message);
        }

        @Override
        public void onFailure(Throwable t) {
            connection.unsubscribe(this);
            synchronized (mine) {
                for (Long session: Iterables.consumingIterable(mine)) {
                    BackendSessionListener listener = sessions.get(session);
                    if ((listener != null) && (listener.dispatcher() == this)) {
                        listener.stop();
                    }
                }
            }
        }
        
        protected void handleMessageSessionOpen(final MessageSessionOpenRequest message) {
            BackendSessionListener listener = sessions.get(message.getIdentifier());
            if (listener != null) {
                if (listener.dispatcher() == this) {
                    onSuccess(MessagePacket.of(MessageSessionOpenResponse.of(message.getIdentifier(), Futures.getUnchecked(listener.session().client().session()))));
                    return;
                } else {
                    if (sessions.remove(message.getIdentifier(), listener)) {
                        listener.session().client().stop();
                    } else {
                        handleMessageSessionOpen(message);
                        return;
                    }
                }
            }
            ListenableFuture<ShardedClientExecutor<? extends ProtocolConnection<? super ClientSession, ? extends edu.uw.zookeeper.protocol.Operation.Response, ?, ?, ?>>> client;
            try {
                client = clientFactory.apply(message);
            } catch (Exception e) {
                client = Futures.immediateFailedFuture(e);
            }
            Futures.addCallback(new SessionOpenTask(Pair.create(message, client)), this, SameThreadExecutor.getInstance());

        }
        
        protected void handleMessageSessionRequest(MessageSessionRequest message) {
            ShardedClientRequestMessage<?> request = (ShardedClientRequestMessage<?>) message.getMessage();
            try {
                BackendSessionListener listener = sessions.get(message.getIdentifier());
                if (listener != null) { 
                    if (listener.dispatcher() == this) {
                        Futures.addCallback(listener.submit(request), this, SameThreadExecutor.getInstance());
                    } else {
                        throw new KeeperException.SessionMovedException();
                    }
                } else {
                    throw new KeeperException.SessionExpiredException();
                }
            } catch (KeeperException e) {
                onSuccess(
                        MessagePacket.of(
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
                Futures.addCallback(future, this, SameThreadExecutor.getInstance());
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
        
        protected class SessionOpenTask extends PromiseTask<Pair<MessageSessionOpenRequest, ? extends ListenableFuture<? extends ShardedClientExecutor<?>>>, MessagePacket<MessageSessionOpenResponse>> implements Runnable, Callable<Optional<MessagePacket<MessageSessionOpenResponse>>> {
            
            private final CallablePromiseTask delegate;
            
            public SessionOpenTask(Pair<MessageSessionOpenRequest, ? extends ListenableFuture<? extends ShardedClientExecutor<?>>> task) {
                super(task, SettableFuturePromise.<MessagePacket<MessageSessionOpenResponse>>create());
                this.delegate = CallablePromiseTask.create(this, this);
                task.second().addListener(this, SameThreadExecutor.getInstance());
            }

            @Override
            public void run() {
                delegate.run();
            }
            
            @Override
            public synchronized Optional<MessagePacket<MessageSessionOpenResponse>> call() throws Exception {
                if (task().second().isDone()) {
                    BackendSessionExecutor session = BackendSessionExecutor.create(task().first().getIdentifier(), task().second().get());
                    assert (session.client().session().isDone());
                    ConnectMessage.Response response = session.client().session().get();
                    // FIXME check if this connection is still valid
                    BackendSessionListener listener = new BackendSessionListener(session);
                    BackendSessionListener existing = sessions.putIfAbsent(task().first().getIdentifier(), listener);
                    if (existing != null) {
                        listener.stop();
                        throw new UnsupportedOperationException();
                    }
                    mine.add(listener.session().session());
                    return Optional.of(MessagePacket.of(
                            MessageSessionOpenResponse.of(
                                    session.session(), response)));
                }
                return Optional.absent();
            }
        }

        protected class BackendSessionListener implements Function<ShardedResponseMessage<?>, MessagePacket<MessageSessionResponse>>, FutureCallback<MessagePacket>, SessionListener, TaskExecutor<ShardedClientRequestMessage<?>, MessagePacket<MessageSessionResponse>> {

            protected final BackendSessionExecutor session;
            
            public BackendSessionListener(
                    BackendSessionExecutor client) {
                this.session = client;
                session().client().subscribe(this);
            }
            
            public BackendSessionExecutor session() {
                return session;
            }
            
            public ServerPeerConnectionDispatcher dispatcher() {
                return ServerPeerConnectionDispatcher.this;
            }
            
            public void stop() {
                session().client().unsubscribe(this);
                session().client().stop();
                sessions.remove(session(), this);
            }

            @Override
            public MessagePacket<MessageSessionResponse> apply(ShardedResponseMessage<?> response) {
                return MessagePacket.of(MessageSessionResponse.of(
                        session().session(), response));
            }

            @Override
            public ListenableFuture<MessagePacket<MessageSessionResponse>> submit(ShardedClientRequestMessage<?> request) {
                ListenableFuture<MessagePacket<MessageSessionResponse>> future = Futures.transform(session.submit(request), this, SameThreadExecutor.getInstance());
                Futures.addCallback(future, this, SameThreadExecutor.getInstance());
                return future;
            }

            @Override
            public void onSuccess(MessagePacket result) {
                dispatcher().onSuccess(result);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("{}", this, t);
                stop();
            }

            @Override
            public void handleAutomatonTransition(
                    Automaton.Transition<ProtocolState> transition) {
                switch (transition.to()) {
                case DISCONNECTED:
                    session().client().unsubscribe(this);
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
                onSuccess(apply((ShardedResponseMessage<?>) notification));
            }
        }
    }
}
