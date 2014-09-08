package edu.uw.zookeeper.safari.peer.protocol;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ForwardingConnection;
import edu.uw.zookeeper.protocol.PingTask;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.safari.Identifier;

@SuppressWarnings("rawtypes")
public abstract class PeerConnection<T extends Connection<? super MessagePacket,? extends MessagePacket,?>, C extends PeerConnection<T,C>> extends ForwardingConnection<MessagePacket,MessagePacket,T,C> implements Connection.Listener<MessagePacket> {

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
        this.heartbeat = SendHeartbeat.create(this, TimeOutParameters.milliseconds(timeOut.value(TimeUnit.MILLISECONDS)), executor);
        this.timeOut = TimeOutActor.create(TimeOutParameters.milliseconds(timeOut.value(TimeUnit.MILLISECONDS)), executor, logger);
        
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
                future.addListener(this, MoreExecutors.directExecutor());
            }
        }
        
        @Override
        public void run() {
            for (ListenableFuture<Void> future: timeOuts) {
                if (future.isDone()) {
                    if (!future.isCancelled()) {
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
    
    protected static class SendHeartbeat extends PingTask<MessagePacket,MessagePacket,PeerConnection<?,?>> {

        public static SendHeartbeat create(
                PeerConnection<?,?> connection,
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            Logger logger = LogManager.getLogger(SendHeartbeat.class);
            SendHeartbeat task = new SendHeartbeat(
                    connection,
                    parameters, 
                    executor,
                    Sets.<Pair<Runnable,Executor>>newHashSet(),
                    SettableFuturePromise.<Void>create(),
                    logger);
            LoggingFutureListener.listen(logger, task);
            return task;
        }
        
        protected SendHeartbeat(
                PeerConnection<?,?> connection,
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                Set<Pair<Runnable,Executor>> listeners,
                Promise<Void> promise,
                Logger logger) {
            super(MessagePacket.of(MessageHeartbeat.getInstance()), connection, parameters, executor, listeners, promise, logger);
        }

        @Override
        public void handleConnectionRead(MessagePacket message) {
        }
    }
}