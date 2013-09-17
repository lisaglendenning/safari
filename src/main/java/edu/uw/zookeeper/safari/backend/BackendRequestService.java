package edu.uw.zookeeper.safari.backend;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.IDisconnectRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.data.Volume;
import edu.uw.zookeeper.safari.data.VolumeCache;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.safari.peer.protocol.ServerPeerConnections;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.PeerConnection.ServerPeerConnection;

@DependsOn({BackendConnectionsService.class})
public class BackendRequestService<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends DependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            TypeLiteral<BackendRequestService<?>> generic = new TypeLiteral<BackendRequestService<?>>() {};
            bind(BackendRequestService.class).to(generic);
        }

        @Provides @Singleton
        public BackendRequestService<?> getBackendRequestService(
                Injector injector,
                BackendConnectionsService<?> connections,
                ServerPeerConnections peers,
                VolumeCache volumes,
                ScheduledExecutorService executor) throws Exception {
            return BackendRequestService.newInstance(
                    injector, volumes, connections, peers, executor);
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(BackendConnectionsService.module());
        }
    }
    
    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> BackendRequestService<C> newInstance(
            Injector injector,
            VolumeCache volumes,
            BackendConnectionsService<C> connections,
            ServerPeerConnections peers,
            ScheduledExecutorService executor) {
        BackendRequestService<C> instance = new BackendRequestService<C>(
                injector,
                connections, 
                peers, 
                newVolumePathLookup(volumes), 
                VolumeShardedOperationTranslators.of(
                        newVolumeIdLookup(volumes)),
                executor);
        instance.new Advertiser(injector, MoreExecutors.sameThreadExecutor());
        return instance;
    }
    
    public static Function<ZNodeLabel.Path, Identifier> newVolumePathLookup(
            final VolumeCache volumes) {
        return new Function<ZNodeLabel.Path, Identifier>() {
            @Override
            public Identifier apply(ZNodeLabel.Path input) {
                return volumes.get(input).getId();
            }
        };
    }
    
    public static Function<Identifier, Volume> newVolumeIdLookup(
            final VolumeCache volumes) {
        return new Function<Identifier, Volume>() {
            @Override
            public Volume apply(Identifier input) {
                return volumes.get(input);
            }
        };
    }

    protected final BackendConnectionsService<C> connections;
    protected final ConcurrentMap<ServerPeerConnection<?>, ServerPeerConnectionListener> peers;
    protected final ConcurrentMap<Long, ServerPeerConnectionDispatcher.BackendClient> clients;
    protected final ShardedOperationTranslators translator;
    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final ServerPeerConnectionListener listener;
    protected final ScheduledExecutorService executor;
    
    protected BackendRequestService(
            Injector injector,
            BackendConnectionsService<C> connections,
            ServerPeerConnections peers,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ShardedOperationTranslators translator,
            ScheduledExecutorService executor) {
        super(injector);
        this.connections = connections;
        this.lookup = lookup;
        this.translator = translator;
        this.executor = executor;
        this.peers = new MapMaker().makeMap();
        this.clients = new MapMaker().makeMap();
        this.listener = new ServerPeerConnectionListener(peers);
    }
    
    public ServerPeerConnectionDispatcher.BackendClient get(Long sessionId) {
        return clients.get(sessionId);
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        ClientConnectionExecutor<C> client = ClientConnectionExecutor.newInstance(
                ConnectMessage.Request.NewRequest.newInstance(), 
                connections.get().get(),
                executor);
        Control.createPrefix(Materializer.newInstance(
                BackendSchema.getInstance().get(), 
                JacksonModule.getSerializer(),
                client));
        client.submit(Records.newInstance(IDisconnectRequest.class)).get();
        client.stop();
        
        listener.start();
    }
    
    @Override
    protected void shutDown() throws Exception {
        listener.stop();
        
        super.shutDown();
    }
    
    protected ListenableFuture<ShardedClientConnectionExecutor<C>> connect(
            MessageSessionOpenRequest request) {
        return Futures.transform(
                connections.get(), 
                new SessionOpenTask(request));
    }

    protected class SessionOpenTask implements Function<C, ShardedClientConnectionExecutor<C>> {

        protected final MessageSessionOpenRequest task;
        
        public SessionOpenTask(
                MessageSessionOpenRequest task) {
            this.task = task;
        }
        
        @Override
        public ShardedClientConnectionExecutor<C> apply(C connection) {
            ConnectMessage.Request request;
            if (task.getValue() instanceof ConnectMessage.Request.NewRequest) {
                request = ConnectMessage.Request.NewRequest.newInstance(
                        TimeValue.create(
                                Long.valueOf(task.getValue().getTimeOut()), 
                                TimeUnit.MILLISECONDS), 
                            connections.zxids().get());
            } else {
                request = ConnectMessage.Request.RenewRequest.newInstance(
                        task.getValue().toSession(), connections.zxids().get());
            }
            
            return ShardedClientConnectionExecutor.newInstance(
                    translator, 
                    lookup, 
                    request, 
                    connection,
                    executor);
        }
    }

    public class Advertiser extends Service.Listener {

        protected final Injector injector;
        
        public Advertiser(Injector injector, Executor executor) {
            this.injector = injector;
            addListener(this, executor);
        }
        
        @Override
        public void running() {
            Materializer<?> materializer = injector.getInstance(ControlMaterializerService.class).materializer();
            Identifier myEntity = injector.getInstance(PeerConfiguration.class).getView().id();
            BackendView view = injector.getInstance(BackendConfiguration.class).getView();
            try {
                BackendConfiguration.advertise(myEntity, view, materializer);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    protected class ServerPeerConnectionListener {
        
        protected final ServerPeerConnections connections;
        protected final ConcurrentMap<ServerPeerConnection<?>, ServerPeerConnectionDispatcher> dispatchers;
        
        public ServerPeerConnectionListener(
                ServerPeerConnections connections) {
            this.connections = connections;
            this.dispatchers = new MapMaker().makeMap();
        }
        
        public void start() {
            connections.register(this);
            for (ServerPeerConnection<?> c: connections) {
                handleConnection(c);
            }
        }
        
        public void stop() {
            try {
                connections.unregister(this);
            } catch (IllegalArgumentException e) {}
        }
        
        @Subscribe
        public void handleConnection(ServerPeerConnection<?> connection) {
            ServerPeerConnectionDispatcher d = new ServerPeerConnectionDispatcher(connection);
            if (dispatchers.putIfAbsent(connection, d) == null) {
                connection.register(d);
            }
        }
    }
    
    protected class ServerPeerConnectionDispatcher extends Factories.Holder<ServerPeerConnection<?>> {
    
        public ServerPeerConnectionDispatcher(ServerPeerConnection<?> connection) {
            super(connection);
        }

        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    get().unregister(this);
                } catch (IllegalArgumentException e) {}
                listener.dispatchers.remove(get(), this);
            }
        }
        
        @Subscribe
        public void handlePeerMessage(MessagePacket message) {
            switch (message.first().type()) {
            case MESSAGE_TYPE_HANDSHAKE:
            case MESSAGE_TYPE_HEARTBEAT:
                break;
            case MESSAGE_TYPE_SESSION_OPEN_REQUEST:
                handleMessageSessionOpen(message.getBody(MessageSessionOpenRequest.class));
                break;
            case MESSAGE_TYPE_SESSION_REQUEST:
                handleMessageSessionRequest(message.getBody(MessageSessionRequest.class));
                break;
            default:
                throw new AssertionError(message.toString());
            }
        }
        
        protected void handleMessageSessionOpen(MessageSessionOpenRequest message) {
            long sessionId = message.getIdentifier();
            BackendClient client = clients.get(sessionId);
            if (client == null) {
                ListenableFuture<ShardedClientConnectionExecutor<C>> connection = connect(message);
                client = new BackendClient(message, connection);
                BackendClient prev = clients.putIfAbsent(sessionId, client);
                if (prev != null) {
                    throw new AssertionError();
                }
            }
            SessionOpenResponseTask.create(message, get(), client.getClient());
        }

        protected void handleMessageSessionRequest(MessageSessionRequest message) {
            BackendClient client = clients.get(message.getIdentifier());
            if (client != null) {
                ListenableFuture<Message.ServerResponse<?>> future;
                try {
                    future = client.submit(message.getValue());
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                if (message.getValue().record().opcode() == OpCode.CLOSE_SESSION) {
                    future.addListener(client.new RemoveTask(), MoreExecutors.sameThreadExecutor());
                }
            } else {
                // TODO
                throw new UnsupportedOperationException();
            }
        }

        protected class BackendClient {
            
            protected final MessageSessionOpenRequest frontend;
            protected final ListenableFuture<ShardedClientConnectionExecutor<C>> client;
            
            public BackendClient(
                    MessageSessionOpenRequest frontend,
                    ListenableFuture<ShardedClientConnectionExecutor<C>> client) {
                this.frontend = frontend;
                this.client = client;
                
                client.addListener(new RegisterTask(), MoreExecutors.sameThreadExecutor());
            }
            
            public MessageSessionOpenRequest getFrontend() {
                return frontend;
            }
            
            public ListenableFuture<ShardedClientConnectionExecutor<C>> getClient() {
                return client;
            }

            public ListenableFuture<Message.ServerResponse<?>> submit(Operation.Request request) throws InterruptedException, ExecutionException {
                return client.get().submit(request);
            }
    
            @Subscribe
            public void handleTransition(Automaton.Transition<?> event) {
                if (Connection.State.CONNECTION_CLOSED == event.to()) {
                    if (client.isDone()) {
                        try {
                            client.get().unregister(this);
                        } catch (Exception e) {
                        }
                    }
                }
            }
            
            @Subscribe
            public void handleResponse(ShardedResponseMessage<?> message) {
                get().write(MessagePacket.of(MessageSessionResponse.of(getFrontend().getIdentifier(), message)));
            }

            protected class RegisterTask implements Runnable {
                @Override
                public void run() {
                    ShardedClientConnectionExecutor<C> result;
                    try {
                        result = client.get();
                    } catch (Exception e) {
                        // TODO
                        throw Throwables.propagate(e);
                    }
                    result.register(BackendClient.this);
                }
            }
            
            protected class RemoveTask implements Runnable {
                @Override
                public void run() {
                    clients.remove(frontend.getIdentifier(), BackendClient.this);
                }
            }
        
        }
    } 
    
    protected static class SessionOpenResponseTask<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends PromiseTask<MessageSessionOpenRequest, MessagePacket> implements Runnable, FutureCallback<MessagePacket> {
        
        public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>>
        SessionOpenResponseTask<C> create(
                MessageSessionOpenRequest request,
                Connection<MessagePacket> connection,
                ListenableFuture<ShardedClientConnectionExecutor<C>> client) {
            Promise<MessagePacket> promise = SettableFuturePromise.create();
            return new SessionOpenResponseTask<C>(request, connection, client, promise);
        }
        
        protected final Connection<MessagePacket> connection;
        protected final ListenableFuture<ShardedClientConnectionExecutor<C>> client;
        
        public SessionOpenResponseTask(
                MessageSessionOpenRequest request,
                Connection<MessagePacket> connection,
                ListenableFuture<ShardedClientConnectionExecutor<C>> client,
                Promise<MessagePacket> promise) {
            super(request, promise);
            this.connection = connection;
            this.client = client;
            
            client.addListener(this, MoreExecutors.sameThreadExecutor());
        }
        
        @Override
        public void run() {
            if (isDone()) {
                return;
            }
            if (! this.client.isDone()) {
                return;
            }
            ShardedClientConnectionExecutor<C> client;
            try {
                client = this.client.get();
            } catch (Exception e) { 
                setException(e);
                return;
            }
            if (! client.session().isDone()) {
                client.session().addListener(this, MoreExecutors.sameThreadExecutor());
                return;
            }
            ConnectMessage.Response response;
            try {
                response = client.session().get();
            } catch (Exception e) { 
                setException(e);
                return;
            }
            ListenableFuture<MessagePacket> future = connection.write(
                    MessagePacket.of(
                            MessageSessionOpenResponse.of(
                                    task().getIdentifier(), response)));
            Futures.addCallback(future, this);
        }

        @Override
        public void onSuccess(MessagePacket result) {
            set(result);
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}
