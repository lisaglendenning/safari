package edu.uw.zookeeper.safari.peer.protocol;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.peer.IdentifierSocketAddress;

public class ClientPeerConnections extends PeerConnections<ClientPeerConnection<?>> implements ClientConnectionFactory<ClientPeerConnection<?>> {

    @SuppressWarnings("rawtypes")
    public static ClientPeerConnections newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            CachedFunction<Identifier, ControlSchema.Peers.Entity.PeerAddress> lookup,
            ClientConnectionFactory<? extends Connection<? super MessagePacket,? extends MessagePacket,?>> connections) {
        return new ClientPeerConnections(identifier, timeOut, executor, lookup, connections, new StrongConcurrentSet<ConnectionsListener<? super ClientPeerConnection<?>>>());
    }
    
    protected final MessagePacket<MessageHandshake> handshake;
    protected final CachedFunction<Identifier, ControlSchema.Peers.Entity.PeerAddress> addressLookup;
    protected final CachedLookup<Identifier, ClientPeerConnection<?>> lookups;
    
    @SuppressWarnings("rawtypes")
    public ClientPeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            CachedFunction<Identifier, ControlSchema.Peers.Entity.PeerAddress> lookup,
            ClientConnectionFactory<? extends Connection<? super MessagePacket,? extends MessagePacket,?>> connections,
            IConcurrentSet<ConnectionsListener<? super ClientPeerConnection<?>>> listeners) {
        super(identifier, timeOut, executor, connections, listeners);
        this.handshake = MessagePacket.of(MessageHandshake.of(identifier));
        this.addressLookup = lookup;
        this.lookups = CachedLookup.fromCache(
                peers, 
                SharedLookup.create(
                        new AsyncFunction<Identifier, ClientPeerConnection<?>>() {
                            @Override
                            public ListenableFuture<ClientPeerConnection<?>> apply(
                                    Identifier peer) throws Exception {
                                ListenableFuture<ControlSchema.Peers.Entity.PeerAddress> lookupFuture = addressLookup.apply(peer);
                                ConnectionTask connection = new ConnectionTask(lookupFuture);
                                try {
                                    return new ConnectTask(peer, connection);
                                } catch (Exception e) {
                                    connection.cancel(true);
                                    throw e;
                                }
                            }
                        }));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ClientConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> connections() {
        return (ClientConnectionFactory<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>>) connections;
    }
    
    @Override
    public ListenableFuture<ClientPeerConnection<?>> connect(
            SocketAddress remoteAddress) {
        return connect(((IdentifierSocketAddress) remoteAddress).getIdentifier());
    }

    public ListenableFuture<ClientPeerConnection<?>> connect(Identifier peer) {
        try {
            return asLookup().apply(peer);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    public CachedFunction<Identifier, ClientPeerConnection<?>> asLookup() {
        return lookups.asLookup();
    }

    protected ListenableFuture<MessagePacket<MessageHandshake>> handshake(ClientPeerConnection<?> peer) {
        return peer.write(handshake);
    }

    @Override
    public ClientPeerConnection<?> put(ClientPeerConnection<?> v) {
        ClientPeerConnection<?> prev = super.put(v);
        handshake(v);
        return prev;
    }

    @Override
    public ClientPeerConnection<?> putIfAbsent(ClientPeerConnection<?> v) {
        ClientPeerConnection<?> prev = super.putIfAbsent(v);
        if (prev == null) {
            handshake(v);
        }
        return prev;
    }

    @SuppressWarnings("rawtypes")
    protected class ConnectionTask extends RunnablePromiseTask<ListenableFuture<ControlSchema.Peers.Entity.PeerAddress>, Connection<? super MessagePacket, ? extends MessagePacket, ?>> implements FutureCallback<Connection<? super MessagePacket, ? extends MessagePacket, ?>> {
        
        protected ListenableFuture<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>> future;

        public ConnectionTask(
                ListenableFuture<ControlSchema.Peers.Entity.PeerAddress> task) {
            this(task, PromiseTask.<Connection<? super MessagePacket, ? extends MessagePacket, ?>>newPromise());
        }
        
        public ConnectionTask(
                ListenableFuture<ControlSchema.Peers.Entity.PeerAddress> task, 
                Promise<Connection<? super MessagePacket, ? extends MessagePacket, ?>> delegate) {
            super(task, delegate);
            this.future = null;
            
            task().addListener(this, MoreExecutors.sameThreadExecutor());
        }
        
        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancel = super.cancel(mayInterruptIfRunning);
            if (cancel) {
                task().cancel(mayInterruptIfRunning);
                if (future != null) {
                    future.cancel(mayInterruptIfRunning);
                }
            }
            return cancel;
        }

        @Override
        public synchronized boolean setException(Throwable t) {
            boolean setException = super.setException(t);
            if (setException) {
                task().cancel(true);
                if (future != null) {
                    future.cancel(true);
                }
            }
            return setException;
        }
        
        @Override
        public synchronized Optional<Connection<? super MessagePacket, ? extends MessagePacket, ?>> call() throws Exception {
            if (task().isDone()) {
                if (task().isCancelled()) {
                    cancel(true);
                } else {
                    if (future == null) {
                        ControlSchema.Peers.Entity.PeerAddress peer = task().get();
                        future = connections().connect(peer.get().get());
                        Futures.addCallback(future, this);
                    }
                }
            }
            return Optional.absent();
        }

        @Override
        public void onSuccess(Connection<? super MessagePacket, ? extends MessagePacket, ?> result) {
            if (! set(result)) {
                result.close();
            }
        }
        
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }

    @SuppressWarnings("rawtypes")
    protected class ConnectTask extends PromiseTask<Identifier, ClientPeerConnection<?>> implements FutureCallback<Connection<? super MessagePacket, ? extends MessagePacket, ?>> {
    
        protected final ConnectionTask connection;
        
        public ConnectTask(
                Identifier task,
                ConnectionTask connection) {
            this(task, connection, PromiseTask.<ClientPeerConnection<?>>newPromise());
        }

        public ConnectTask(
                Identifier task,
                ConnectionTask connection,
                Promise<ClientPeerConnection<?>> delegate) {
            super(task, delegate);
            this.connection = connection;
            
            Futures.addCallback(connection, this);
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancel = super.cancel(mayInterruptIfRunning);
            if (cancel) {
                connection.cancel(mayInterruptIfRunning);
            }
            return cancel;
        }
    
        @Override
        public boolean set(ClientPeerConnection<?> result) {
            boolean set;
            ClientPeerConnection<?> prev = putIfAbsent(result);
            if (prev != null) {
                if (result.delegate() != prev.delegate()) {
                    result.close();
                }
                set = super.set(prev);
            } else {
                set = super.set(result);
            }
            return set;
        }

        @Override
        public void onSuccess(Connection<? super MessagePacket, ? extends MessagePacket, ?> result) {
            try {
                if (! isDone()) {
                    set(ClientPeerConnection.create(
                            identifier(), task(), result, timeOut, executor));
                } else {
                    result.close();
                }
            } catch (Exception e) {
                result.close();
                onFailure(e);
            }
        }
    
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}