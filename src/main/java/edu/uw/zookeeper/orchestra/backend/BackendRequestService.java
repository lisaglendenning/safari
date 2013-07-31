package edu.uw.zookeeper.orchestra.backend;

import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.VolumeCache;
import edu.uw.zookeeper.orchestra.VolumeLookupService;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ServerPeerConnection;
import edu.uw.zookeeper.orchestra.peer.PeerConnectionsService;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageHandshake;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionResponse;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.TimeValue;

@DependsOn({ControlMaterializerService.class, BackendConnectionsService.class})
public class BackendRequestService<C extends Connection<? super Operation.Request>> extends DependentService.SimpleDependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(BackendConnectionsService.module());
            TypeLiteral<BackendRequestService<?>> generic = new TypeLiteral<BackendRequestService<?>>() {};
            bind(BackendRequestService.class).to(generic);
            bind(generic).to(new TypeLiteral<BackendRequestService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>() {});
        }

        @Provides @Singleton
        public BackendRequestService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getBackendRequestService(
                ServiceLocator locator,
                BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections,
                VolumeLookupService volumes,
                DependentServiceMonitor monitor,
                Executor executor) throws Exception {
            return monitor.add(BackendRequestService.newInstance(
                    locator, volumes.get(), connections, executor));
        }
    }
    
    public static <C extends Connection<? super Operation.Request>> BackendRequestService<C> newInstance(
            ServiceLocator locator,
            final VolumeCache volumes,
            BackendConnectionsService<C> connections,
            Executor executor) {

        Function<ZNodeLabel.Path, Identifier> lookup = new Function<ZNodeLabel.Path, Identifier>() {
            @Override
            public Identifier apply(ZNodeLabel.Path input) {
                return volumes.get(input).getId();
            }
        };
        VolumeShardedOperationTranslators translator = new VolumeShardedOperationTranslators(
                new Function<Identifier, Volume>() {
                    @Override
                    public Volume apply(Identifier input) {
                        return volumes.get(input);
                    }
                });
        BackendRequestService<C> instance = new BackendRequestService<C>(
                locator, connections, lookup, translator, executor);
        instance.new Advertiser(MoreExecutors.sameThreadExecutor());
        return instance;
    }

    protected final BackendConnectionsService<C> connections;
    protected final ConcurrentMap<ServerPeerConnection, ServerPeerConnectionListener> peers;
    protected final ConcurrentMap<Long, ServerPeerConnectionListener.BackendClient> clients;
    protected final ShardedOperationTranslators translator;
    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final ConnectionTask connectionTask;
    protected final Executor executor;
    
    protected BackendRequestService(
            ServiceLocator locator,
            BackendConnectionsService<C> connections,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ShardedOperationTranslators translator,
            Executor executor) {
        super(locator);
        this.connections = connections;
        this.peers = new MapMaker().makeMap();
        this.clients = new MapMaker().makeMap();
        this.lookup = lookup;
        this.translator = translator;
        this.connectionTask = new ConnectionTask();
        this.executor = executor;
    }
    
    public ServerPeerConnectionListener.BackendClient get(Long sessionId) {
        return clients.get(sessionId);
    }

    @Subscribe
    public void handleServerPeerConnection(ServerPeerConnection peer) {
        new ServerPeerConnectionListener(peer);
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        PeerConnectionsService<?> peers = locator().getInstance(PeerConnectionsService.class);
        peers.servers().register(this);
        for (Entry<Identifier, ServerPeerConnection> e: peers.servers().entrySet()) {
            handleServerPeerConnection(e.getValue());
        }
    }
    
    @Override
    protected void shutDown() throws Exception {
        try {
            locator().getInstance(PeerConnectionsService.class).servers().unregister(this);
        } catch (IllegalArgumentException e) {}
        
        super.shutDown();
    }
    
    protected ListenableFuture<ShardedClientConnectionExecutor<C>> connect(
            MessageSessionOpenRequest request) {
        ListenableFutureTask<C> task = 
                ListenableFutureTask.create(connectionTask);
        ListenableFuture<ShardedClientConnectionExecutor<C>> future = Futures.transform(
                task, 
                new SessionOpenTask(request));
        executor.execute(task);
        return future;
    }
    
    protected class ConnectionTask implements Callable<C> {
        @Override
        public C call() {
            return connections.get();
        }
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
            if (task.delegate() instanceof ConnectMessage.Request.NewRequest) {
                request = ConnectMessage.Request.NewRequest.newInstance(
                        TimeValue.create(
                                Long.valueOf(task.delegate().getTimeOut()), 
                                TimeUnit.MILLISECONDS), 
                            connections.zxids().get());
            } else {
                request = ConnectMessage.Request.RenewRequest.newInstance(
                        task.delegate().toSession(), connections.zxids().get());
            }
            
            return ShardedClientConnectionExecutor.newInstance(
                    translator, 
                    lookup, 
                    ConnectTask.create(connection, request), 
                    connection);
        }
    }

    public class Advertiser implements Service.Listener {

        public Advertiser(Executor executor) {
            addListener(this, executor);
        }
        
        @Override
        public void starting() {
        }

        @Override
        public void running() {
            Materializer<?,?> materializer = locator().getInstance(ControlMaterializerService.class).materializer();
            Identifier myEntity = locator().getInstance(PeerConfiguration.class).getView().id();
            BackendView view = locator().getInstance(BackendConfiguration.class).getView();
            try {
                BackendConfiguration.advertise(myEntity, view, materializer);
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

    protected class ServerPeerConnectionListener implements Reference<ServerPeerConnection> {
    
        protected final ServerPeerConnection peer;
        
        public ServerPeerConnectionListener(ServerPeerConnection peer) {
            this.peer = peer;
            if (peers.putIfAbsent(peer, this) == null) {
                peer.register(this);
            }
            // TODO: remove from map when peer closes?
        }
        
        @Override
        public ServerPeerConnection get() {
            return peer;
        }
        
        @Subscribe
        public void handlePeerMessage(MessagePacket message) {
            switch (message.first().type()) {
            case MESSAGE_TYPE_HANDSHAKE:
                handleMessageHandshake(message.getBody(MessageHandshake.class));
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
        
        public void handleMessageHandshake(MessageHandshake second) {
            assert(second.getId() == get().localAddress().getIdentifier());
        }

        public void handleMessageSessionOpen(MessageSessionOpenRequest message) {
            long sessionId = message.delegate().getSessionId();
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

        public void handleMessageSessionRequest(MessageSessionRequest message) {
            BackendClient client = clients.get(message.getSessionId());
            if (client != null) {
                ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> future;
                try {
                    future = client.submit(message.getRequest());
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                if (message.getRequest().getRecord().getOpcode() == OpCode.CLOSE_SESSION) {
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

            public ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> submit(Operation.Request request) throws InterruptedException, ExecutionException {
                ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> future = client.get().submit(request);
                return future;
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
                get().write(MessagePacket.of(MessageSessionResponse.of(getFrontend().getSessionId(), message)));
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
                    clients.remove(frontend.getSessionId(), BackendClient.this);
                }
            }
        
        }
    } 
    
    protected static class SessionOpenResponseTask<C extends Connection<? super Operation.Request>> extends PromiseTask<MessageSessionOpenRequest, MessagePacket> implements Runnable, FutureCallback<MessagePacket> {
        
        public static <C extends Connection<? super Operation.Request>>
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
                                    task().getSessionId(), response)));
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
