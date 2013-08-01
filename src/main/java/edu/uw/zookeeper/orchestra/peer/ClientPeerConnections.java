package edu.uw.zookeeper.orchestra.peer;

import java.net.SocketAddress;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageHandshake;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;

public class ClientPeerConnections<C extends Connection<? super MessagePacket>> extends PeerConnections<C, ClientPeerConnection<Connection<? super MessagePacket>>> implements ClientConnectionFactory<ClientPeerConnection<Connection<? super MessagePacket>>> {

    protected final Identifier identifier;
    protected final MessagePacket handshake;
    protected final ConnectionTask connectionTask;
    protected final CachedFunction<Identifier, ControlSchema.Peers.Entity.PeerAddress> lookup;
    
    public ClientPeerConnections(
            Identifier identifier,
            Materializer<?,?> control,
            ClientConnectionFactory<C> connections) {
        super(connections);
        this.identifier = identifier;
        this.handshake = MessagePacket.of(MessageHandshake.of(identifier));
        this.lookup = ControlSchema.Peers.Entity.PeerAddress.lookup(control);
        this.connectionTask = new ConnectionTask();
    }

    @Override
    public ClientConnectionFactory<C> connections() {
        return (ClientConnectionFactory<C>) connections;
    }

    public Identifier identifier() {
        return identifier;
    }
    
    public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connect(Identifier identifier) {
        ClientPeerConnection<Connection<? super MessagePacket>> connection = get(identifier);
        if (connection != null) {
            return Futures.immediateFuture(connection);
        } else {
            ListenableFuture<ControlSchema.Peers.Entity.PeerAddress> lookupFuture;
            try {
                lookupFuture = lookup.apply(identifier);
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
            ListenableFuture<C> connectionFuture = Futures.transform(lookupFuture, connectionTask);
            ConnectTask connectTask = new ConnectTask(identifier);
            Futures.addCallback(connectionFuture, connectTask);
            return connectTask;
        }
    }

    @Override
    public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connect(
            SocketAddress remoteAddress) {
        IdentifierSocketAddress id = (IdentifierSocketAddress) remoteAddress;
        return connect(id.getIdentifier());
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
    
    public CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectFunction() {
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
    
    protected class ConnectionTask implements AsyncFunction<ControlSchema.Peers.Entity.PeerAddress, C> {
        @Override
        public ListenableFuture<C> apply(
                ControlSchema.Peers.Entity.PeerAddress input) throws Exception {
            return connections().connect(input.get().get());
        }
    }

    protected class ConnectTask extends PromiseTask<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> implements FutureCallback<C> {
    
        public ConnectTask(Identifier task) {
            this(task, PromiseTask.<ClientPeerConnection<Connection<? super MessagePacket>>>newPromise());
        }

        public ConnectTask(Identifier task,
                Promise<ClientPeerConnection<Connection<? super MessagePacket>>> delegate) {
            super(task, delegate);
        }
    
        @Override
        public void onSuccess(C result) {
            try {
                if (! isDone()) {
                    ClientPeerConnection<Connection<? super MessagePacket>> peer = new ClientPeerConnection<Connection<? super MessagePacket>>(identifier(), task(), result);
                    ClientPeerConnection<Connection<? super MessagePacket>> prev = putIfAbsent(peer);
                    if (prev != null) {
                        set(prev);
                    } else {
                        set(peer);
                    }
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