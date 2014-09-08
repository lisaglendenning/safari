package edu.uw.zookeeper.safari.frontend;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.LoggingFutureListener;
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
            LoggingFutureListener.listen(logger, promise);
            NotEquals task = new NotEquals(current, promise);
            task.run();
            future = Futures.transform(task, connector);
            future.addListener(this, MoreExecutors.directExecutor());
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
    public String toString() {
        return MoreObjects.toStringHelper(this).add("region", region).add("connection", connection).toString();
    }
    
    @Override
    protected synchronized ListenableFuture<ClientPeerConnection<?>> delegate() {
        if ((connection != null) && 
                (connection.state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
            return Futures.<ClientPeerConnection<?>>immediateFuture(connection);
        } else {
            if (future == null) {
                run();
                return delegate();
            } else {
                return future;
            }
        }
    }

    protected class NotEquals extends PromiseTask<Identifier, Identifier> implements FutureCallback<Identifier>, Runnable {

        public NotEquals(Identifier task, Promise<Identifier> delegate) {
            super(checkNotNull(task), delegate);
        }

        @Override
        public synchronized void run() {
            if (! isDone()) {
                try {
                    Futures.addCallback(selector.apply(region), this);
                } catch (Exception e) {
                    onFailure(e);
                }
            }
        }

        @Override
        public void onSuccess(Identifier result) {
            if (task.equals(result)) {
                // try again
                run();
            } else {
                if (result != null) {
                    set(result);
                } else {
                    onFailure(new IllegalStateException(String.valueOf(region)));
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}
