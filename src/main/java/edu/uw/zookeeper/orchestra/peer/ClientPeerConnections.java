package edu.uw.zookeeper.orchestra.peer;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.common.CachedFunction;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.common.RunnablePromiseTask;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageHandshake;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;

public class ClientPeerConnections<C extends Connection<? super MessagePacket>> extends PeerConnections<C, ClientPeerConnection<Connection<? super MessagePacket>>> implements ClientConnectionFactory<ClientPeerConnection<Connection<? super MessagePacket>>> {

    protected final Identifier identifier;
    protected final MessagePacket handshake;
    protected final CachedFunction<Identifier, ControlSchema.Peers.Entity.PeerAddress> addressLookup;
    protected final ConcurrentMap<Identifier, ConnectTask> lookups;
    
    public ClientPeerConnections(
            Identifier identifier,
            CachedFunction<Identifier, ControlSchema.Peers.Entity.PeerAddress> lookup,
            ClientConnectionFactory<C> connections) {
        super(connections);
        this.identifier = identifier;
        this.handshake = MessagePacket.of(MessageHandshake.of(identifier));
        this.addressLookup = lookup;
        this.lookups = new MapMaker().makeMap();
    }

    @Override
    public ClientConnectionFactory<C> connections() {
        return (ClientConnectionFactory<C>) connections;
    }

    public Identifier identifier() {
        return identifier;
    }
    
    @Override
    public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connect(
            SocketAddress remoteAddress) {
        return connect(((IdentifierSocketAddress) remoteAddress).getIdentifier());
    }

    public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connect(Identifier identifier) {
        ClientPeerConnection<Connection<? super MessagePacket>> connection = get(identifier);
        if (connection != null) {
            return Futures.immediateFuture(connection);
        } else {
            ConnectTask connectTask = lookups.get(identifier);
            if (connectTask != null) {
                return connectTask;
            } else {
                try {
                    ListenableFuture<ControlSchema.Peers.Entity.PeerAddress> lookupFuture = addressLookup.apply(identifier);
                    ConnectionTask connectionTask = new ConnectionTask(lookupFuture);
                    try {
                        connectTask = new ConnectTask(identifier, connectionTask);
                        ConnectTask prev = lookups.putIfAbsent(identifier, connectTask);
                        if (prev == null) {
                            Futures.addCallback(connectionTask, connectTask);
                        } else {
                            connectTask.cancel(true);
                            connectTask = prev;
                        }
                        return connectTask;
                    } catch (Exception e) {
                        connectionTask.cancel(true);
                        throw e;
                    }
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }
        }
    }

    public CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> asLookup() {
        return CachedFunction.create(
                new Function<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>>() {
                    @Override
                    public @Nullable ClientPeerConnection<Connection<? super MessagePacket>> apply(Identifier ensemble) {
                        return get(ensemble);
                    }                    
                }, 
                new AsyncFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>>() {
                    @Override
                    public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> apply(Identifier ensemble) {
                        return connect(ensemble);
                    }
                });
    }

    protected ListenableFuture<MessagePacket> handshake(ClientPeerConnection<Connection<? super MessagePacket>> peer) {
        return peer.write(handshake);
    }

    protected ClientPeerConnection<Connection<? super MessagePacket>> put(ClientPeerConnection<Connection<? super MessagePacket>> v) {
        ClientPeerConnection<Connection<? super MessagePacket>> prev = put(v.remoteAddress().getIdentifier(), v);
        handshake(v);
        return prev;
    }

    protected ClientPeerConnection<Connection<? super MessagePacket>> putIfAbsent(ClientPeerConnection<Connection<? super MessagePacket>> v) {
        ClientPeerConnection<Connection<? super MessagePacket>> prev = putIfAbsent(v.remoteAddress().getIdentifier(), v);
        if (prev == null) {
            handshake(v);
        }
        return prev;
    }
    
    protected class ConnectionTask extends RunnablePromiseTask<ListenableFuture<ControlSchema.Peers.Entity.PeerAddress>, C> implements FutureCallback<C> {
        
        protected ListenableFuture<C> future;

        public ConnectionTask(
                ListenableFuture<ControlSchema.Peers.Entity.PeerAddress> task) {
            this(task, PromiseTask.<C>newPromise());
        }
        
        public ConnectionTask(
                ListenableFuture<ControlSchema.Peers.Entity.PeerAddress> task, 
                Promise<C> delegate) {
            super(task, delegate);
            this.future = null;
            
            task.addListener(this, MoreExecutors.sameThreadExecutor());
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
        public synchronized Optional<C> call() throws Exception {
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
        public void onSuccess(C result) {
            if (! set(result)) {
                result.close();
            }
        }
        
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }

    protected class ConnectTask extends PromiseTask<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> implements FutureCallback<C> {
    
        protected final ConnectionTask connection;
        
        public ConnectTask(
                Identifier task,
                ConnectionTask connection) {
            this(task, connection, PromiseTask.<ClientPeerConnection<Connection<? super MessagePacket>>>newPromise());
        }

        public ConnectTask(
                Identifier task,
                ConnectionTask connection,
                Promise<ClientPeerConnection<Connection<? super MessagePacket>>> delegate) {
            super(task, delegate);
            this.connection = connection;
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancel = super.cancel(mayInterruptIfRunning);
            if (cancel) {
                connection.cancel(mayInterruptIfRunning);
                lookups.remove(task(), this);
            }
            return cancel;
        }
    
        @Override
        public boolean set(ClientPeerConnection<Connection<? super MessagePacket>> result) {
            boolean set;
            ClientPeerConnection<Connection<? super MessagePacket>> prev = putIfAbsent(result);
            if (prev != null) {
                if (result.delegate() != prev.delegate()) {
                    result.close();
                }
                set = super.set(prev);
            } else {
                set = super.set(result);
            }
            if (set) {
                lookups.remove(task(), this);
            }
            return set;
        }

        @Override
        public boolean setException(Throwable t) {
            boolean setException = super.setException(t);
            if (setException) {
                lookups.remove(task(), this);
            }
            return setException;
        }
        
        @Override
        public void onSuccess(C result) {
            try {
                if (! isDone()) {
                    set(ClientPeerConnection.<Connection<? super MessagePacket>>create(
                            identifier(), task(), result));
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