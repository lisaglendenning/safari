package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;

public class ClientPeerConnectionDispatchers {

    public static ClientPeerConnectionDispatchers newInstance(
            AsyncFunction<Identifier, ? extends ClientPeerConnection<?>> connections) {
        return new ClientPeerConnectionDispatchers(connections);
    }
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final AsyncFunction<Identifier, ? extends ClientPeerConnection<?>> connections;
    protected final CachedLookup<Identifier, ClientPeerConnectionDispatcher> dispatchers;
    
    protected ClientPeerConnectionDispatchers(
            AsyncFunction<Identifier, ? extends ClientPeerConnection<?>> connections) {
        this.logger = LogManager.getLogger(getClass());
        this.connections = connections;
        this.dispatchers = CachedLookup.shared(
                new AsyncFunction<Identifier, ClientPeerConnectionDispatcher>() {
                    @Override
                    public ListenableFuture<ClientPeerConnectionDispatcher> apply(
                            Identifier input) throws Exception {
                        Promise<ClientPeerConnectionDispatcher> promise = SettableFuturePromise.create();
                        promise = LoggingPromise.create(logger, promise);
                        return new CreateDispatcherTask(input, promise);
                    }
                });
    }
    
    public CachedFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers() {
        return dispatchers.asLookup();
    }

    protected class CreateDispatcherTask extends PromiseTask<Identifier, ClientPeerConnectionDispatcher> implements FutureCallback<ClientPeerConnection<?>> {

        public CreateDispatcherTask(
                Identifier task,
                Promise<ClientPeerConnectionDispatcher> delegate) {
            super(task, delegate);
            
            try {
                Futures.addCallback(connections.apply(task), this);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onSuccess(ClientPeerConnection<?> connection) {
            ClientPeerConnectionDispatcher dispatcher = new ClientPeerConnectionDispatcher(task, connection);
            set(dispatcher);
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
    
    @SuppressWarnings("rawtypes")
    protected class ClientPeerConnectionDispatcher implements Connection.Listener<MessagePacket>, Eventful<PeerConnectionListener> {

        protected final Identifier identifier;
        protected final ClientPeerConnection<?> connection;
        protected final Connector connector;
        protected final ConcurrentMap<Long, PeerConnectionListener> listeners;
        
        public ClientPeerConnectionDispatcher(
                Identifier identifier,
                ClientPeerConnection<?> connection) {
            this.identifier = identifier;
            this.connection = connection;
            this.connector = new Connector();
            this.listeners = new MapMaker().makeMap();
            
            dispatchers.asCache().put(identifier, this);
            connection.subscribe(this);
            if (connection.state().compareTo(Connection.State.CONNECTION_CLOSED) >= 0) {
                handleConnectionState(Automaton.Transition.create(
                        connection.state(), connection.state()));
            }
        }
        
        public Identifier getIdentifier() {
            return identifier;
        }
        
        public ClientPeerConnection<?> connection() {
            return connection;
        }
        
        public ListenableFuture<MessageSessionOpenResponse> connect(MessageSessionOpenRequest request) {
            return connector.submit(request);
        }

        @Override
        public void subscribe(PeerConnectionListener listener) {
            listeners.put(listener.session().id(), listener);
        }

        @Override
        public boolean unsubscribe(PeerConnectionListener listener) {
            return listeners.remove(listener.session().id(), listener);
        }

        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            Iterable<PeerConnectionListener> listeners = this.listeners.values();
            if (Connection.State.CONNECTION_CLOSED == state.to()) {
                connection.unsubscribe(this);
                dispatchers.asCache().remove(identifier, this);
                listeners = Iterables.consumingIterable(listeners);
            }
            connector.handleConnectionState(state);
            for (PeerConnectionListener listener: listeners) {
                listener.handleConnectionState(state);
            }
        }

        @Override
        public void handleConnectionRead(MessagePacket message) {
            switch (message.getHeader().type()) {
            case MESSAGE_TYPE_SESSION_RESPONSE:
            {
                MessageSessionResponse body = (MessageSessionResponse) message.getBody();
                PeerConnectionListener listener = listeners.get(body.getIdentifier());
                if (listener != null) {
                    listener.handleConnectionRead(body.getValue());
                } else {
                    logger.warn("Ignoring {}", body);
                }
                break;
            }
            default:
                break;
            }
            connector.handleConnectionRead(message);
        }
        
        protected class Connector implements TaskExecutor<MessageSessionOpenRequest, MessageSessionOpenResponse>, Connection.Listener<MessagePacket> {

            protected final ConcurrentMap<Long, OpenSessionTask> connects;
            
            public Connector() {
                this.connects = new MapMaker().makeMap();
            }
            
            @Override
            public ListenableFuture<MessageSessionOpenResponse> submit(
                    MessageSessionOpenRequest request) {
                Promise<MessageSessionOpenResponse> promise = SettableFuturePromise.create();
                promise = LoggingPromise.create(logger, promise);
                OpenSessionTask task = new OpenSessionTask(request, promise);
                return task;
            }
            
            @Override
            public void handleConnectionState(Automaton.Transition<Connection.State> state) {
                if (Connection.State.CONNECTION_CLOSED == state.to()) {
                    for (OpenSessionTask connect: Iterables.consumingIterable(connects.values())) {
                        connect.cancel(true);
                    }
                }
            }

            @Override
            public void handleConnectionRead(MessagePacket message) {
                switch (message.getHeader().type()) {
                case MESSAGE_TYPE_SESSION_OPEN_RESPONSE:
                {
                    MessageSessionOpenResponse body = (MessageSessionOpenResponse) message.getBody();
                    OpenSessionTask task = connects.get(body.getIdentifier());
                    if (task != null) {
                        task.set(body);
                    } else {
                        logger.warn("Ignoring {}", body);
                    }
                    break;
                }
                default:
                    break;
                }
            }
            
            protected class OpenSessionTask extends PromiseTask<MessageSessionOpenRequest, MessageSessionOpenResponse> implements Runnable {

                protected volatile ListenableFuture<MessagePacket<MessageSessionOpenRequest>> write;

                public OpenSessionTask(
                        MessageSessionOpenRequest request,
                        Promise<MessageSessionOpenResponse> promise) {
                    super(request, promise);
                    this.write = null;
                    
                    if (connects.putIfAbsent(task.getIdentifier(), this) == null) {
                        this.addListener(this, SAME_THREAD_EXECUTOR);
                        run();
                    } else {
                        throw new UnsupportedOperationException();
                    }
                }
                
                @Override
                public synchronized void run() {
                    if (! isDone()) {
                        try {
                            if (write == null) {
                                write = connection.write(MessagePacket.of(task));
                                write.addListener(this, SAME_THREAD_EXECUTOR);
                            } else if (write.isDone()) {
                                write.get();
                            }
                        } catch (Exception e) {
                            setException(e);
                        }
                    } else {
                        if (isCancelled()) {
                            if (write != null) {
                                write.cancel(false);
                            }
                        }
                        connects.remove(task.getIdentifier(), this);
                    }
                }
            }
        }
    }
}
