package edu.uw.zookeeper.safari.peer.protocol;

import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.CachedFunction;
import edu.uw.zookeeper.common.CachedLookup;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.SharedLookup;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;

public class ClientPeerConnections extends PeerConnections<ClientPeerConnection<?>> implements ClientConnectionFactory<ClientPeerConnection<?>> {

    @SuppressWarnings("rawtypes")
    public static ClientPeerConnections newInstance(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            CachedFunction<Identifier, ServerInetAddressView> lookup,
            ClientConnectionFactory<? extends Connection<? super MessagePacket,? extends MessagePacket,?>> connections) {
        return new ClientPeerConnections(identifier, timeOut, executor, lookup, connections);
    }
    
    protected final MessagePacket<MessageHandshake> handshake;
    protected final CachedFunction<Identifier, ServerInetAddressView> addressLookup;
    protected final CachedLookup<Identifier, ClientPeerConnection<?>> lookups;
    
    @SuppressWarnings("rawtypes")
    public ClientPeerConnections(
            Identifier identifier,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            CachedFunction<Identifier, ServerInetAddressView> lookup,
            ClientConnectionFactory<? extends Connection<? super MessagePacket,? extends MessagePacket,?>> connections) {
        super(identifier, timeOut, executor, connections, ImmutableList.<Service.Listener>of());
        this.handshake = MessagePacket.valueOf(MessageHandshake.of(identifier));
        this.addressLookup = lookup;
        this.lookups = CachedLookup.fromCache(
                peers, 
                SharedLookup.create(
                        new AsyncFunction<Identifier, ClientPeerConnection<?>>() {
                            @Override
                            public ListenableFuture<ClientPeerConnection<?>> apply(
                                    Identifier peer) throws Exception {
                                ListenableFuture<ServerInetAddressView> lookupFuture = addressLookup.apply(peer);
                                Connect connection = new Connect(lookupFuture, SettableFuturePromise.<Connection<? super MessagePacket, ? extends MessagePacket, ?>>create());
                                LoggingFutureListener.listen(logger, connection);
                                try {
                                    return new ConnectTask(peer, connection);
                                } catch (Exception e) {
                                    connection.cancel(true);
                                    throw e;
                                }
                            }
                        }),
                LogManager.getLogger(getClass()));
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

    @SuppressWarnings("rawtypes")
    protected ListenableFuture<MessagePacket> handshake(ClientPeerConnection<?> peer) {
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
    protected class Connect extends PromiseTask<ListenableFuture<ServerInetAddressView>,Connection<? super MessagePacket, ? extends MessagePacket, ?>> implements Callable<Optional<Connection<? super MessagePacket, ? extends MessagePacket, ?>>>, FutureCallback<Connection<? super MessagePacket, ? extends MessagePacket, ?>>, Runnable {

        protected final CallablePromiseTask<Connect, Connection<? super MessagePacket, ? extends MessagePacket, ?>> delegate;
        protected Optional<? extends ListenableFuture<? extends Connection<? super MessagePacket, ? extends MessagePacket, ?>>> future;
        
        public Connect(
                ListenableFuture<ServerInetAddressView> address,
                Promise<Connection<? super MessagePacket, ? extends MessagePacket, ?>> promise) {
            super(address, promise);
            this.delegate = CallablePromiseTask.create(this, this);
            this.future = Optional.absent();
            address.addListener(this, MoreExecutors.directExecutor());
            addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public synchronized void run() {
            if (isDone()) {
                if (isCancelled()) {
                    task().cancel(false);
                    if (future.isPresent()) {
                        future.get().cancel(false);
                    }
                } else {
                    try {
                        get();
                    } catch (Exception e) {
                        task().cancel(false);
                        if (future.isPresent()) {
                            future.get().cancel(false);
                        }
                    }
                }
            } else {
                delegate.run();
            }
        }

        @Override
        public synchronized Optional<Connection<? super MessagePacket, ? extends MessagePacket, ?>> call() throws Exception {
            if (task().isDone()) {
                if (task().isCancelled()) {
                    cancel(true);
                } else {
                    if (!future.isPresent()) {
                        ServerInetAddressView peer = task().get();
                        future = Optional.of(connections().connect(peer.get()));
                        Futures.addCallback(future.get(), this);
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
    
        protected final Connect connection;
        
        public ConnectTask(
                Identifier task,
                Connect connection) {
            this(task, connection, PromiseTask.<ClientPeerConnection<?>>newPromise());
        }

        public ConnectTask(
                Identifier task,
                Connect connection,
                Promise<ClientPeerConnection<?>> promise) {
            super(task, promise);
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