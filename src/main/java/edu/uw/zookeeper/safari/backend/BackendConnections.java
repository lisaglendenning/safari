package edu.uw.zookeeper.safari.backend;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.Message.ClientSession;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.Response;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class BackendConnections extends AbstractModule {

    public static BackendConnections create() {
        return new BackendConnections();
    }
    
    protected BackendConnections() {}
    
    @Override
    protected void configure() {
        bind(Key.get(ServerInetAddressView.class, Backend.class)).to(Key.get(ServerInetAddressView.class, Storage.class));
        bind(Key.get(new TypeLiteral<ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>(){}, Backend.class)).to(Key.get(new TypeLiteral<ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>(){}, Storage.class));
        bind(Key.get(new TypeLiteral<Materializer<StorageZNode<?>,?>>(){}, Backend.class)).to(Key.get(new TypeLiteral<Materializer<StorageZNode<?>,Message.ServerResponse<?>>>(){}, Backend.class));
        bind(Key.get(new TypeLiteral<Materializer<StorageZNode<?>,Message.ServerResponse<?>>>(){}, Backend.class)).to(Key.get(new TypeLiteral<Materializer<StorageZNode<?>,Message.ServerResponse<?>>>(){}, Storage.class));
        bind(Key.get(ZxidReference.class, Backend.class)).to(Key.get(ZxidReference.class, Storage.class));
        bind(Key.get(ZxidTracker.class, Backend.class)).to(Key.get(ZxidTracker.class, Storage.class));
    }
    
    @Provides @Backend
    public ListenableFuture<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> newBackendConnection(
            @Backend ServerInetAddressView address,
            @Backend ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
            final @Backend ZxidTracker zxids) {
        Services.startAndWait(connections);
        return Futures.transform(
                connections.connect(address.get()), 
                new Function<ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>, ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>() {
                    @Override
                    public ProtocolConnection<? super ClientSession, ? extends Response, ?, ?, ?> apply(
                            ProtocolConnection<? super ClientSession, ? extends Response, ?, ?, ?> input) {
                        ZxidTracker.listener(zxids, input);
                        return input;
                    }
                });
    }
    
    @Provides @Singleton
    public AsyncFunction<MessageSessionOpenRequest, MessageSessionOpenRequest> newSessionOpenToConnectRequest(
            final @Backend Materializer<StorageZNode<?>,?> materializer,
            final @Backend ZxidReference zxids) {
        final SessionOpenToConnectRequest openToRequest = SessionOpenToConnectRequest.create(zxids, materializer);
        return new AsyncFunction<MessageSessionOpenRequest, MessageSessionOpenRequest>() {
            @Override
            public ListenableFuture<MessageSessionOpenRequest> apply(
                    final MessageSessionOpenRequest request) throws Exception {
                return Futures.transform(openToRequest.apply(request),
                        new Function<ConnectMessage.Request, MessageSessionOpenRequest>() {
                    @Override
                    public MessageSessionOpenRequest apply(ConnectMessage.Request message) {
                        return MessageSessionOpenRequest.of(request.getIdentifier(), message);
                    }
                });
            }
        };
    }
    
    @Provides @Singleton
    public AsyncFunction<MessageSessionOpenRequest, ? extends ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> newSessionOpenToConnect(
            final @Backend Provider<ListenableFuture<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> connections,
            final @Backend Materializer<StorageZNode<?>,?> materializer) {
            return new AsyncFunction<MessageSessionOpenRequest, ConnectTask<ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>() {
                    @Override
                    public ListenableFuture<ConnectTask<ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> apply(
                            final MessageSessionOpenRequest request) {
                        return Futures.transform(
                                connections.get(), 
                                new AsyncFunction<ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>, ConnectTask<ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>() {
                                    @Override
                                    public ListenableFuture<ConnectTask<ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> apply(ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?> connection) {
                                        ListenableFuture<ConnectTask<ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> task = 
                                                LoggingFutureListener.listen(
                                                        LogManager.getLogger(BackendConnections.class),
                                                            RegisterSessionTask.<Operation.Response, ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>connect( 
                                                                request.getIdentifier(), 
                                                                request.getMessage(), 
                                                                connection,
                                                                materializer.codec()));
                                        return task;
                                    }
                                });
                    }
        };
    }
}
