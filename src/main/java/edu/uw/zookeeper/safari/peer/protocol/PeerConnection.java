package edu.uw.zookeeper.safari.peer.protocol;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.IdentifierSocketAddress;

@SuppressWarnings("rawtypes")
public abstract class PeerConnection<T extends Connection<? super MessagePacket,? extends MessagePacket,?>, C extends PeerConnection<T,C>> extends ForwardingConnection<MessagePacket,MessagePacket,T,C> implements Connection.Listener<MessagePacket> {

    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final T delegate;
    protected final Identifier localIdentifier;
    protected final Identifier remoteIdentifier;
    protected final SendHeartbeat heartbeat;
    protected final TimeOutActor<MessagePacket, Void> timeOut;
    
    protected PeerConnection(
            Identifier localIdentifier,
            Identifier remoteIdentifier,
            T delegate,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        this.logger = LogManager.getLogger(getClass());
        this.delegate = delegate;
        this.localIdentifier = localIdentifier;
        this.remoteIdentifier = remoteIdentifier;
        this.heartbeat = SendHeartbeat.create(this, TimeOutParameters.create(timeOut), executor);
        this.timeOut = TimeOutActor.create(TimeOutParameters.create(timeOut), executor);
        
        subscribe(this);
        
        new TimeOutListener();
        
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
    public T delegate() {
        return delegate;
    }

    @Override
    public <V extends MessagePacket> ListenableFuture<V> write(V input) {
        heartbeat.send(input);
        return super.write(input);
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> state) {
        if (Connection.State.CONNECTION_CLOSED == state.to()) {
            unsubscribe(this);
            heartbeat.stop();
            timeOut.stop();
        }
    }

    @Override
    public void handleConnectionRead(MessagePacket message) {
        timeOut.send(message);
    }

    protected class TimeOutListener implements Runnable {

        protected final List<ListenableFuture<Void>> timeOuts;
        
        public TimeOutListener() {
            this.timeOuts = ImmutableList.<ListenableFuture<Void>>of(heartbeat, timeOut);
            for (ListenableFuture<Void> future: timeOuts) {
                future.addListener(this, SAME_THREAD_EXECUTOR);
            }
        }
        
        @Override
        public void run() {
            for (ListenableFuture<Void> future: timeOuts) {
                if (future.isDone()) {
                    if (! future.isCancelled()) {
                        try {
                            future.get();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            close();
                        }
                    }
                }
            }
        }
    }
    
    protected static class SendHeartbeat extends TimeOutActor<MessagePacket,Void> implements FutureCallback<Object>, Connection.Listener<MessagePacket> {

        public static SendHeartbeat create(
                PeerConnection<?,?> connection,
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            Logger logger = LogManager.getLogger(TimeOutActor.class);
            return new SendHeartbeat(
                    connection,
                    parameters, 
                    executor,
                    new WeakConcurrentSet<Pair<Runnable,Executor>>(),
                    LoggingPromise.create(logger, SettableFuturePromise.<Void>create()),
                    logger);
        }
        
        private final MessagePacket<MessageHeartbeat> heartbeat;
        private final WeakReference<? extends PeerConnection<?,?>> connection;
        
        protected SendHeartbeat(
                PeerConnection<?,?> connection,
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                IConcurrentSet<Pair<Runnable,Executor>> listeners,
                Promise<Void> promise,
                Logger logger) {
            super(parameters, executor, listeners, promise, logger);
            this.connection = new WeakReference<PeerConnection<?,?>>(connection);
            this.heartbeat = MessagePacket.of(MessageHeartbeat.getInstance());
            
            connection.subscribe(this);
        }

        @Override
        public void handleConnectionRead(MessagePacket message) {
        }

        @Override
        public void handleConnectionState(
                Automaton.Transition<Connection.State> state) {
            switch (state.to()) {
            case CONNECTION_CLOSING:
            case CONNECTION_CLOSED:
                stop();
                break;
            default:
                break;
            }
        }
        
        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            promise.setException(t);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("connection", connection.get()).toString();
        }

        @Override
        protected void doRun() {
            PeerConnection<?,?> connection = this.connection.get();
            if (isDone() || (connection == null) || (connection.state() == Connection.State.CONNECTION_CLOSED)) {
                stop();
                return;
            }
            
            if (parameters.remaining() < parameters.getTimeOut() / 2) {
                parameters.touch();
                Futures.addCallback(
                        connection.write(heartbeat), 
                        this,
                        SAME_THREAD_EXECUTOR);
            }
        }

        @Override
        protected synchronized void doSchedule() {
            if (parameters.getTimeOut() != NEVER_TIMEOUT) {
                if ((connection.get() != null) && !isDone() && !scheduler.isShutdown()) {
                    // somewhat arbitrary...
                    long tick = Math.max(parameters.remaining() / 2, 0);
                    scheduled = scheduler.schedule(this, tick, parameters.getUnit());
                } else {
                    stop();
                }
            } else {
                state.compareAndSet(State.SCHEDULED, State.WAITING);
            }
        }

        @Override
        protected synchronized void doStop() {
            PeerConnection<?,?> connection = this.connection.get();
            if (connection != null) {
                connection.unsubscribe(this);
            }
            
            super.doStop();
        }
        
        @Override
        protected Logger logger() {
            return logger;
        }
    }
}