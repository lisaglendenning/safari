package edu.uw.zookeeper.safari.frontend;

import java.nio.channels.ClosedChannelException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;

public class EstablishBackendSessionTask extends PromiseTask<Identifier, Session> implements Runnable, FutureCallback<MessagePacket> {
    
    protected final Session frontend;
    protected final Optional<Session> existing;
    protected final ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connection;
    protected volatile ListenableFuture<MessagePacket> writeFuture;
    
    public EstablishBackendSessionTask(
            Session frontend,
            Optional<Session> existing,
            Identifier ensemble,
            ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connection,
            Promise<Session> promise) throws Exception {
        super(ensemble, promise);
        this.frontend = frontend;
        this.existing = existing;
        this.connection = connection;
        this.writeFuture = null;
        
        connection.addListener(this, MoreExecutors.sameThreadExecutor());
    }
    
    public Session frontend() {
        return frontend;
    }
    
    public Optional<Session> existing() {
        return existing;
    }

    public ListenableFuture<ClientPeerConnection<Connection<? super MessagePacket>>> connection() {
        return connection;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean doCancel = super.cancel(mayInterruptIfRunning);
        if (doCancel) {
            if (! connection.isDone()) {
                connection.cancel(mayInterruptIfRunning);
            }
            if ((writeFuture != null) && !writeFuture.isDone()) {
                writeFuture.cancel(mayInterruptIfRunning);
            }
        }
        return doCancel;
    }
    
    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        assert(connection.isDone());
        if (event.to() == Connection.State.CONNECTION_CLOSED) {
            Futures.getUnchecked(connection).unregister(this);
            if (! isDone()) {
                setException(new ClosedChannelException());
            }
        }
    }
    
    @Subscribe
    public void handleMessage(MessagePacket message) {
        assert(connection.isDone());
        switch (message.first().type()) {
        case MESSAGE_TYPE_SESSION_OPEN_RESPONSE:
        {
            MessageSessionOpenResponse response = message.getBody(MessageSessionOpenResponse.class);
            if (response.getIdentifier() != frontend.id()) {
                break;
            }
            Futures.getUnchecked(connection).unregister(this);
            if (! isDone()) {
                if (response.getValue() instanceof ConnectMessage.Response.Valid) {
                    set(response.getValue().toSession());
                } else {
                    setException(new KeeperException.SessionExpiredException());
                }
            }
            break;
        }
        default:
            break;
        }
    }
    
    @Override
    public void run() {
        if (connection.isDone()) {
            ClientPeerConnection<?> c;
            try {
                c = connection.get();
            } catch (Exception e) {
                if (! isDone()) {
                    setException(e);
                }
                return;
            }

            c.register(this);
            
            ConnectMessage.Request request;
            if (existing.isPresent()) {
                request = ConnectMessage.Request.RenewRequest.newInstance(
                            existing.get(), 0L);
            } else {
                request = ConnectMessage.Request.NewRequest.newInstance(
                        frontend.parameters().timeOut(), 0L);
            }
            
            if (writeFuture == null) {
                writeFuture = c.write(MessagePacket.of(
                        MessageSessionOpenRequest.of(
                                frontend.id(), request)));
                Futures.addCallback(writeFuture, this);
            }
        }
    }

    @Override
    public void onSuccess(MessagePacket result) {
    }

    @Override
    public void onFailure(Throwable t) {
        setException(t);
    }
}