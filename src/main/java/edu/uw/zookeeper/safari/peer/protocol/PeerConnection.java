package edu.uw.zookeeper.safari.peer.protocol;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.listener.Handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.IdentifierSocketAddress;

public class PeerConnection<C extends Connection<? super MessagePacket<?>>> extends ForwardingConnection<MessagePacket<?>> {

    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
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
        this.logger = LogManager.getLogger(getClass());
        this.delegate = delegate;
        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;
        this.heartbeat = new SendHeartbeat(TimeOutParameters.create(timeOut), executor);
        this.timeOut = new TimeOutRemote(TimeOutParameters.create(timeOut), executor);
        
        subscribe(this);
        
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
    public <V extends MessagePacket<?>> ListenableFuture<V> write(V input) {
        heartbeat.send(input);
        return super.write(input);
    }

    @Handler
    public void handleTransitionEvent(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            try {
                unsubscribe(this);
            } catch (Exception e) {}
            heartbeat.stop();
            timeOut.stop();
        }
    }

    @Handler
    public void handleMessage(MessagePacket<?> message) {
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
                parameters.touch();
                Futures.addCallback(
                        write(MessagePacket.of(MessageHeartbeat.getInstance())), 
                        this,
                        SAME_THREAD_EXECUTOR);
            }
        }

        @Override
        protected synchronized void doSchedule() {
            if (parameters.getTimeOut() != NEVER_TIMEOUT) {
                if (!executor.isShutdown()) {
                    long tick = Math.max(parameters.remaining() / 2, 0);
                    future = executor.schedule(this, tick, parameters.getUnit());
                } else {
                    stop();
                }
            } else {
                state.compareAndSet(State.SCHEDULED, State.WAITING);
            }
        }

        @Override
        protected Logger logger() {
            return logger;
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

        @Override
        protected Logger logger() {
            return logger;
        }
    }
}