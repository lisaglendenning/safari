package edu.uw.zookeeper.safari.backend;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ForwardingPromise.SimpleForwardingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;

/**
 * Creates StorageSchema.Safari.Sessions.Session znode mapping frontend session to backend session and password.
 */
public final class RegisterSessionTask<O extends Operation.Response, C extends ProtocolConnection<? super Message.ClientSession, ? extends O, ?, ?, ?>> extends SimpleForwardingPromise<ConnectTask<C>> implements Runnable, FutureCallback<Operation.Response> {

    public static <O extends Operation.Response, C extends ProtocolConnection<? super Message.ClientSession, ? extends O, ?, ?, ?>> ListenableFuture<ConnectTask<C>> connect(
            final Long frontend,
            final ConnectMessage.Request request,
            final C connection,
            final Serializers.ByteSerializer<? super StorageSchema.Safari.Sessions.Session.Data> serializer) {
        return create(frontend, ConnectTask.connect(request, connection), serializer);
    }
    
    public static <O extends Operation.Response, C extends ProtocolConnection<? super Message.ClientSession, ? extends O, ?, ?, ?>> ListenableFuture<ConnectTask<C>> create(
            final Long frontend,
            final ConnectTask<C> value,
            final Serializers.ByteSerializer<? super StorageSchema.Safari.Sessions.Session.Data> serializer) {
        final Operations.DataBuilder<? extends Records.Request, ?> request = toRequest(frontend, value.task().first());
        RegisterSessionTask<O,C> instance = new RegisterSessionTask<O,C>(request, serializer, value, SettableFuturePromise.<ConnectTask<C>>create());
        instance.run();
        return instance;
    }

    public static Operations.DataBuilder<? extends Records.Request, ?> toRequest(
            final Long frontend,
            final ConnectMessage.Request request) {
        final Operations.DataBuilder<? extends Records.Request, ?> builder;
        if (request instanceof ConnectMessage.Request.NewRequest) {
            builder = Operations.Requests.create().setMode(CreateMode.EPHEMERAL);
        } else {
            builder = Operations.Requests.setData();
        }
        final ZNodePath path = StorageSchema.Safari.Sessions.Session.pathOf(frontend);
        ((Operations.PathBuilder<?,?>) builder).setPath(path);
        return builder;
    }

    private final Operations.DataBuilder<? extends Records.Request, ?> request;
    private final Serializers.ByteSerializer<? super StorageSchema.Safari.Sessions.Session.Data> serializer;
    private final ConnectTask<C> value;
    
    public RegisterSessionTask(
            Operations.DataBuilder<? extends Records.Request, ?> request,
            Serializers.ByteSerializer<? super StorageSchema.Safari.Sessions.Session.Data> serializer,
            ConnectTask<C> value,
            Promise<ConnectTask<C>> promise) {
        super(promise);
        this.request = request;
        this.serializer = serializer;
        this.value = value;
    }
    
    @Override
    public void run() {
        if (value.isDone()) {
            try {
                onSuccess(value.get());
            } catch (Exception e) {
                onFailure(e);
            }
        } else {
            value.addListener(this, MoreExecutors.directExecutor());
        }
    }

    @Override
    public void onSuccess(Operation.Response result) {
        if (result instanceof ConnectMessage.Response) {
            final ConnectMessage.Response response = (ConnectMessage.Response) result;
            final StorageSchema.Safari.Sessions.Session.Data data = StorageSchema.Safari.Sessions.Session.Data.valueOf(response.getSessionId(), response.getPasswd());
            final Records.Request request;
            try {
                request = this.request.setData(serializer.toBytes(data)).build();
            } catch (IOException e) {
                onFailure(e);
                return;
            }
            Futures.addCallback(
                    CreateSessionZNode.create(
                            request, 
                            value.task().second(), 
                            response),
                    this);
        } else {
            set(value);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        setException(t);
    }
    
    protected static final class CreateSessionZNode<O extends Operation.Response> extends SimpleForwardingPromise<O> implements FutureCallback<Operation>, Connection.Listener<Operation.Response> {

        public static <O extends Operation.Response> ListenableFuture<O> create(
                final Records.Request request,
                final ProtocolConnection<? super Message.ClientSession, ? extends O, ?, ?, ?> connection,
                final ConnectMessage.Response response) {
            final Message.ClientRequest<?> message = ProtocolRequestMessage.of(
                    XID,
                    request);
            final CreateSessionZNode<O> instance = new CreateSessionZNode<O>(SettableFuturePromise.<O>create());
            connection.subscribe(instance);
            try {
                Futures.addCallback(connection.write(message), instance);
            } finally {
                UnsubscribeWhenDone.listen(instance, connection, instance);
            }
            return instance;
        }

        // magic constant xid
        protected static final int XID = 0;
        
        protected CreateSessionZNode(Promise<O> delegate) {
            super(delegate);
        }

        @Override
        public void handleConnectionState(Automaton.Transition<edu.uw.zookeeper.net.Connection.State> state) {
            if (Connection.State.CONNECTION_CLOSED == state.to()) {
                onFailure(new KeeperException.ConnectionLossException());
            }
        }

        @Override
        public void handleConnectionRead(Operation.Response message) {
            Operation.ProtocolResponse<?> response = (Operation.ProtocolResponse<?>) message;
            if (response.xid() == XID) {
                try {
                    Operations.unlessError(((Operation.ProtocolResponse<?>) message).record());
                } catch (KeeperException e) {
                    setException(e);
                    return;
                }
                onSuccess(message);
            } else {
                assert (OpCodeXid.has(response.xid()));
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onSuccess(Operation result) {
            if (result instanceof Operation.Response) {
                set((O) result);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
        
        protected static final class UnsubscribeWhenDone<O> implements Runnable {

            public static <O, T extends Connection.Listener<? super O>> T listen(
                    ListenableFuture<?> future,
                    Connection<?, ? extends O, ?> connection,
                    T listener) {
                UnsubscribeWhenDone<O> callback = new UnsubscribeWhenDone<O>(future, connection, listener);
                future.addListener(callback, MoreExecutors.directExecutor());
                return listener;
            }
            
            private final ListenableFuture<?> future;
            private final Connection<?, ? extends O, ?> connection;
            private final Connection.Listener<? super O> listener;
            
            protected UnsubscribeWhenDone(
                    ListenableFuture<?> future,
                    Connection<?, ? extends O, ?> connection,
                    Connection.Listener<? super O> listener) {
                this.future = future;
                this.connection = connection;
                this.listener = listener;
            }
            
            @Override
            public void run() {
                if (future.isDone()) {
                    connection.unsubscribe(listener);
                }
            }
        }
    }
}
