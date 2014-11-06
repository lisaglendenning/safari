package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ForwardingPromise.SimpleForwardingPromise;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;

@SuppressWarnings("rawtypes")
public final class RegionClientPeerConnection extends ToStringListenableFuture<ClientPeerConnection<?>> implements Runnable, FutureCallback<ClientPeerConnection<?>>, Connection.Listener<MessagePacket> {

    public static RegionClientPeerConnection create(
            Identifier region,
            AsyncFunction<Identifier, Optional<Identifier>> selector,
            AsyncFunction<Identifier, ClientPeerConnection<?>> connector,
            final Function<? super Runnable, ? extends ScheduledFuture<?>> schedule) {
        return new RegionClientPeerConnection(region, selector, connector, schedule);
    }

    protected final Logger logger;
    protected final Identifier region;
    protected final AsyncFunction<Identifier, ClientPeerConnection<?>> connector;
    protected final Callable<? extends ListenableFuture<? extends Optional<Identifier>>> selector;
    protected final Callable<? extends ScheduledFuture<?>> schedule;
    protected Optional<? extends ListenableFuture<ClientPeerConnection<?>>> future;
    protected Optional<? extends ClientPeerConnection<?>> connection;
    
    protected RegionClientPeerConnection(
            final Identifier region,
            final AsyncFunction<Identifier, Optional<Identifier>> selector,
            final AsyncFunction<Identifier, ClientPeerConnection<?>> connector,
            final Function<? super Runnable, ? extends ScheduledFuture<?>> schedule) {
        this.logger = LogManager.getLogger(getClass());
        this.region = region;
        this.selector = new Callable<ListenableFuture<? extends Optional<Identifier>>>() {
            @Override
            public ListenableFuture<? extends Optional<Identifier>> call()
                    throws Exception {
                return selector.apply(region);
            }
        };
        this.connector = connector;
        this.schedule = new Callable<ScheduledFuture<?>>() {
            @Override
            public ScheduledFuture<?> call() throws Exception {
                return schedule.apply(RegionClientPeerConnection.this);
            }
        };
        this.connection = Optional.absent();
        this.future = Optional.absent();
    }
    
    public Identifier region() {
        return region;
    }

    @Override
    public synchronized void run() {
        if (!future.isPresent()) {
            if (connection.isPresent()) {
                connection.get().unsubscribe(this);
                connection.get().close();
                connection = Optional.absent();
            }
            SelectRegionMember task = LoggingFutureListener.listen(logger, new SelectRegionMember());
            task.run();
            future = Optional.of(Futures.transform(task, connector));
            LoggingFutureListener.listen(logger, this);
            future.get().addListener(this, MoreExecutors.directExecutor());
        } else if (future.get().isDone()) {
            try {
                onSuccess(future.get().get());
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                // we assume that we will be able to connect to a member of the
                // other region eventually, and that regions aren't deleted
                logger.warn("Error connecting to region {}", region, e);
                future = Optional.absent();
                try {
                    schedule.call();
                } catch (Exception e1) {
                    onFailure(e1);
                }
            }
        }
    }

    @Override
    public synchronized void onSuccess(ClientPeerConnection<?> result) {
        connection = Optional.of(result);
        future = Optional.absent();
        connection.get().subscribe(this);
    }

    @Override
    public synchronized void onFailure(Throwable t) {
        future = Optional.of(Futures.<ClientPeerConnection<?>>immediateFailedFuture(t));
    }

    @Override
    public synchronized void handleConnectionState(Automaton.Transition<Connection.State> state) {
        switch (state.to()) {
        case CONNECTION_CLOSED:
        {
            logger.info("Connection to peer {} of region {} closed", connection.get().remoteAddress().getIdentifier(), region);
            run();
            break;
        }
        default:
            break;
        }
    }

    @Override
    public void handleConnectionRead(MessagePacket message) {
    }
    
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        return super.toStringHelper(helper.addValue(region));
    }
    
    @Override
    protected synchronized ListenableFuture<ClientPeerConnection<?>> delegate() {
        if (connection.isPresent() && 
                (connection.get().state().compareTo(Connection.State.CONNECTION_CLOSING) < 0)) {
            return Futures.<ClientPeerConnection<?>>immediateFuture(connection.get());
        } else {
            if (!future.isPresent()) {
                run();
                return delegate();
            } else {
                return future.get();
            }
        }
    }

    protected final class SelectRegionMember extends SimpleForwardingPromise<Identifier> implements FutureCallback<Optional<Identifier>>, Runnable {

        protected SelectRegionMember() {
            super(SettableFuturePromise.<Identifier>create());
        }

        @Override
        public void run() {
            if (!isDone()) {
                try {
                    Futures.addCallback(selector.call(), this);
                } catch (Exception e) {
                    onFailure(e);
                }
            }
        }

        @Override
        public void onSuccess(Optional<Identifier> result) {
            if (result.isPresent()) {
                set(result.get());
            } else {
                onFailure(new IllegalStateException(String.valueOf(region)));
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}
