package edu.uw.zookeeper.orchestra.peer.protocol;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.peer.IdentifierSocketAddress;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;

public class PeerConnection<C extends Connection<? super MessagePacket>> extends ForwardingConnection<MessagePacket> {

    protected final C delegate;
    protected final Identifier localIdentifier;
    protected final Identifier remoteIdentifier;
    protected final SendHeartbeat heartbeat;
    protected final TimeOutRemote timeOut;
    
    public PeerConnection(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            C delegate,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        this.delegate = delegate;
        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;
        this.heartbeat = new SendHeartbeat(TimeOutParameters.create(timeOut), executor);
        this.timeOut = new TimeOutRemote(TimeOutParameters.create(timeOut), executor);
        
        register(this);
        
        this.heartbeat.run();
        this.timeOut.run();
    }

    @Override
    public IdentifierSocketAddress localAddress() {
        return IdentifierSocketAddress.of(localIdentifier, super.localAddress());
    }

    @Override
    public IdentifierSocketAddress remoteAddress() {
        return IdentifierSocketAddress.of(remoteIdentifier, super.remoteAddress());
    }

    @Override
    public C delegate() {
        return delegate;
    }

    @Override
    public <V extends MessagePacket> ListenableFuture<V> write(V input) {
        heartbeat.send(input);
        return super.write(input);
    }

    @Subscribe
    public void handleTransitionEvent(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            unregister(this);
            heartbeat.stop();
            timeOut.stop();
        }
    }
    
    @Subscribe
    public void handleMessage(MessagePacket message) {
        timeOut.send(message);
    }

    protected class SendHeartbeat extends TimeOutActor<Object> implements FutureCallback<Object> {
        
        protected SendHeartbeat(
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            super(parameters, executor);
        }
        
        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            close();
        }

        @Override
        protected void doRun() {
            if (parameters.remaining() < parameters.getTimeOut() / 2) {
                try {
                    Futures.addCallback(
                            write(MessagePacket.of(MessageHeartbeat.getInstance())), 
                            this);
                } catch (Exception e) {
                    close();
                    return;
                }
                parameters.touch();
            }
        }

        @Override
        protected synchronized void doSchedule() {
            if (parameters.getTimeOut() != NEVER_TIMEOUT) {
                long tick = Math.max(parameters.remaining() / 2, 0);
                future = executor.schedule(this, tick, parameters.getUnit());
            } else {
                state.compareAndSet(State.SCHEDULED, State.WAITING);
            }
        }
    }
    
    protected class TimeOutRemote extends TimeOutActor<Object> {
        
        protected TimeOutRemote(
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            super(parameters, executor);
        }

        @Override
        protected void doRun() {
            if (parameters.remaining() <= 0) {
                close();
            }
        }
    }
    
    public static class ClientPeerConnection<C extends Connection<? super MessagePacket>> extends PeerConnection<C> {

        public static <C extends Connection<? super MessagePacket>> ClientPeerConnection<C> create(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection,
                TimeValue timeOut,
                ScheduledExecutorService executor) {
            return new ClientPeerConnection<C>(
                    localIdentifier, remoteIdentifier, connection, timeOut, executor);
        }
        
        public ClientPeerConnection(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection,
                TimeValue timeOut,
                ScheduledExecutorService executor) {
            super(localIdentifier, remoteIdentifier, connection, timeOut, executor);
        }
    }


    public static class ServerPeerConnection<C extends Connection<? super MessagePacket>> extends PeerConnection<C> {

        public static <C extends Connection<? super MessagePacket>> ServerPeerConnection<C> create(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection,
                TimeValue timeOut,
                ScheduledExecutorService executor) {
            return new ServerPeerConnection<C>(localIdentifier, remoteIdentifier, connection, timeOut, executor);
        }
        
        public ServerPeerConnection(
                Identifier localIdentifier,
                Identifier remoteIdentifier,
                C connection,
                TimeValue timeOut,
                ScheduledExecutorService executor) {
            super(localIdentifier, remoteIdentifier, connection, timeOut, executor);
        }
    }
}