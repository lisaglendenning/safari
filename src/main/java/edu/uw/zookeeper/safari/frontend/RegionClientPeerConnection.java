package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;

public class RegionClientPeerConnection extends ForwardingListenableFuture<ClientPeerConnection<?>> implements Runnable {

    public static RegionClientPeerConnection newInstance(
            Identifier region,
            AsyncFunction<Identifier, Identifier> selector,
            AsyncFunction<Identifier, ClientPeerConnection<?>> connector) {
        return new RegionClientPeerConnection(region, selector, connector);
    }

    protected static Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();

    protected final Logger logger;
    protected final Identifier region;
    protected final AsyncFunction<Identifier, ClientPeerConnection<?>> connector;
    protected final AsyncFunction<Identifier, Identifier> selector;
    protected ListenableFuture<ClientPeerConnection<?>> future;
    protected ClientPeerConnection<?> connection;
    
    protected RegionClientPeerConnection(
            Identifier region,
            AsyncFunction<Identifier, Identifier> selector,
            AsyncFunction<Identifier, ClientPeerConnection<?>> connector) {
        this.logger = LogManager.getLogger(getClass());
        this.region = region;
        this.selector = selector;
        this.connector = connector;
        this.connection = null;
        this.future = null;
    }
    
    public Identifier region() {
        return region;
    }

    @Override
    public synchronized void run() {
        if (future == null) {
            Identifier current = (connection == null) ? Identifier.zero() : connection.remoteAddress().getIdentifier();
            connection = null;
            Promise<Identifier> promise = SettableFuturePromise.create();
            promise = LoggingPromise.create(logger, promise);
            NotEquals task = new NotEquals(current, promise);
            task.run();
            future = Futures.transform(task, connector, SAME_THREAD_EXECUTOR);
            future.addListener(this, SAME_THREAD_EXECUTOR);
        } else if (future.isDone()) {
            try {
                connection = future.get();
                future = null;
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } catch (ExecutionException e) {
                // TODO
                throw new UnsupportedOperationException(e);
            }
        }
    }
    
    @Override
    protected synchronized ListenableFuture<ClientPeerConnection<?>> delegate() {
        if ((connection != null) && 
                (connection.state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
            return Futures.<ClientPeerConnection<?>>immediateFuture(connection);
        } else {
            if (future == null) {
                run();
            }
            return future;
        }
    }

    protected class NotEquals extends PromiseTask<Identifier, Identifier> implements FutureCallback<Identifier>, Runnable {

        public NotEquals(Identifier task, Promise<Identifier> delegate) {
            super(task, delegate);
        }

        @Override
        public synchronized void run() {
            if (! isDone()) {
                try {
                    Futures.addCallback(selector.apply(region), this, SAME_THREAD_EXECUTOR);
                } catch (Exception e) {
                    onFailure(e);
                }
            }
        }

        @Override
        public void onSuccess(Identifier result) {
            if (result.equals(task)) {
                // try again
                run();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}
