package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionResponse;

public class ClientPeerConnectionDispatchers implements Supplier<CachedFunction<Identifier, ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher>> {

    public static ClientPeerConnectionDispatchers newInstance(
            AsyncFunction<Identifier, ? extends ClientPeerConnection<?>> connections) {
        return new ClientPeerConnectionDispatchers(connections);
    }
    
    protected final Logger logger;
    protected final AsyncFunction<Identifier, ? extends ClientPeerConnection<?>> connections;
    protected final CachedLookup<Identifier, ClientPeerConnectionDispatcher> dispatchers;
    
    protected ClientPeerConnectionDispatchers(
            final AsyncFunction<Identifier, ? extends ClientPeerConnection<?>> connections) {
        this.logger = LogManager.getLogger(getClass());
        this.connections = connections;
        this.dispatchers = CachedLookup.shared(
                new AsyncFunction<Identifier, ClientPeerConnectionDispatcher>() {
                    @Override
                    public ListenableFuture<ClientPeerConnectionDispatcher> apply(
                            final Identifier input) throws Exception {
                        final Promise<ClientPeerConnectionDispatcher> promise = 
                                LoggingPromise.create(logger, 
                                        SettableFuturePromise.<ClientPeerConnectionDispatcher>create());
                        final ListenableFuture<? extends ClientPeerConnection<?>> connection = connections.apply(input);
                        return new CreateDispatcherTask(Pair.create(input, connection), promise);
                    }
                }, logger);
    }
    
    @Override
    public CachedFunction<Identifier, ClientPeerConnectionDispatcher> get() {
        return dispatchers.asLookup();
    }

    protected class CreateDispatcherTask extends PromiseTask<Pair<Identifier,? extends ListenableFuture<? extends ClientPeerConnection<?>>>, ClientPeerConnectionDispatcher> implements Callable<Optional<ClientPeerConnectionDispatcher>> {

        public CreateDispatcherTask(
                Pair<Identifier,? extends ListenableFuture<? extends ClientPeerConnection<?>>> task,
                Promise<ClientPeerConnectionDispatcher> delegate) {
            super(task, delegate);
            task.second().addListener(CallablePromiseTask.create(this, this), SameThreadExecutor.getInstance());
        }
        
        @Override
        public Optional<ClientPeerConnectionDispatcher> call() throws Exception {
            if (task().second().isDone()) {
                final ClientPeerConnection<?> connection;
                try { 
                    connection = task().second().get();
                } catch (ExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                ClientPeerConnectionDispatcher dispatcher = new ClientPeerConnectionDispatcher(task().first(), connection);
                ClientPeerConnectionDispatcher existing = dispatchers.asCache().putIfAbsent(dispatcher.identifier(), dispatcher);
                if (existing != null) {
                    // FIXME
                    throw new UnsupportedOperationException();
                } else {
                    connection.subscribe(dispatcher);
                    if (connection.state().compareTo(Connection.State.CONNECTION_CLOSED) >= 0) {
                        dispatcher.handleConnectionState(Automaton.Transition.create(
                                connection.state(), connection.state()));
                    }
                }
                return Optional.of(dispatcher);
            }
            return Optional.absent();
        }
    }
    
    @SuppressWarnings("rawtypes")
    public class ClientPeerConnectionDispatcher implements Connection.Listener<MessagePacket>, Eventful<SessionPeerListener> {

        protected final Identifier identifier;
        protected final ClientPeerConnection<?> connection;
        protected final Connector connector;
        protected final ConcurrentMap<Long, SessionPeerListener> listeners;
        
        public ClientPeerConnectionDispatcher(
                Identifier identifier,
                ClientPeerConnection<?> connection) {
            this.identifier = identifier;
            this.connection = connection;
            this.connector = new Connector();
            this.listeners = new MapMaker().makeMap();
        }
        
        public Identifier identifier() {
            return identifier;
        }
        
        public ClientPeerConnection<?> connection() {
            return connection;
        }
        
        public ListenableFuture<MessageSessionOpenResponse> connect(MessageSessionOpenRequest request) {
            return connector.submit(request);
        }

        @Override
        public void subscribe(SessionPeerListener listener) {
            listeners.put(listener.session().id(), listener);
        }

        @Override
        public boolean unsubscribe(SessionPeerListener listener) {
            return listeners.remove(listener.session().id(), listener);
        }

        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            Iterable<SessionPeerListener> listeners = this.listeners.values();
            if (Connection.State.CONNECTION_CLOSED == state.to()) {
                connection.unsubscribe(this);
                dispatchers.asCache().remove(identifier, this);
                listeners = Iterables.consumingIterable(listeners);
            }
            connector.handleConnectionState(state);
            for (SessionPeerListener listener: listeners) {
                listener.handleConnectionState(state);
            }
        }

        @Override
        public void handleConnectionRead(MessagePacket message) {
            switch (message.getHeader().type()) {
            case MESSAGE_TYPE_SESSION_RESPONSE:
            {
                MessageSessionResponse body = (MessageSessionResponse) message.getBody();
                SessionPeerListener listener = listeners.get(body.getIdentifier());
                if (listener != null) {
                    listener.handleConnectionRead(body.getMessage());
                } else {
                    logger.warn("Ignoring {}", body);
                }
                break;
            }
            case MESSAGE_TYPE_SESSION_OPEN_RESPONSE:
            {
                connector.handleConnectionRead(message);
                break;
            }
            default:
                break;
            }
        }
        
        protected class Connector implements TaskExecutor<MessageSessionOpenRequest, MessageSessionOpenResponse>, Connection.Listener<MessagePacket> {

            protected final ConcurrentMap<Long, OpenSessionTask> connects;
            
            public Connector() {
                this.connects = new MapMaker().makeMap();
            }
            
            @Override
            public ListenableFuture<MessageSessionOpenResponse> submit(
                    final MessageSessionOpenRequest request) {
                final Promise<MessageSessionOpenResponse> promise = 
                        LoggingPromise.create(logger, 
                                SettableFuturePromise.<MessageSessionOpenResponse>create());
                final OpenSessionTask task = new OpenSessionTask(request, promise);
                task.run();
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
                MessageSessionOpenResponse body = (MessageSessionOpenResponse) message.getBody();
                OpenSessionTask task = connects.remove(body.getIdentifier());
                if (task != null) {
                    task.set(body);
                } else {
                    logger.warn("Ignoring {}", message);
                }
            }
            
            protected class OpenSessionTask extends PromiseTask<MessageSessionOpenRequest, MessageSessionOpenResponse> implements Runnable {

                private Optional<ListenableFuture<MessagePacket<MessageSessionOpenRequest>>> write;

                public OpenSessionTask(
                        MessageSessionOpenRequest request,
                        Promise<MessageSessionOpenResponse> promise) {
                    super(request, promise);
                    this.write = Optional.absent();
                }
                
                @Override
                public synchronized void run() {
                    if (!isDone()) {
                        try {
                            if (!write.isPresent()) {
                                if (connects.putIfAbsent(task.getIdentifier(), this) == null) {
                                    try {
                                        write = Optional.of(connection.write(MessagePacket.of(task)));
                                        write.get().addListener(this, SameThreadExecutor.getInstance());
                                    } finally {
                                        this.addListener(this, SameThreadExecutor.getInstance());
                                    }
                                } else {
                                    throw new UnsupportedOperationException();
                                }
                            } else if (write.get().isDone()) {
                                write.get();
                            }
                        } catch (Exception e) {
                            setException(e);
                        }
                    } else {
                        if (isCancelled()) {
                            if (write.isPresent()) {
                                write.get().cancel(false);
                            }
                        }
                        connects.remove(task.getIdentifier(), this);
                    }
                }
            }
        }
    }
}
