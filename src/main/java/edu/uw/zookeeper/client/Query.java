package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;

public class Query extends ToStringListenableFuture<List<Operation.ProtocolResponse<?>>> implements Runnable, Callable<Optional<Operation.Error>> {

    public static Query call(
            FixedQuery<?> query,
            AbstractWatchListener listener) {
        return new Query(query, listener);
    }
    
    protected final AbstractWatchListener listener;
    
    protected Query(
            FixedQuery<?> query,
            AbstractWatchListener listener) {
        this(listener, Futures.<Operation.ProtocolResponse<?>>allAsList(query.call()));
        checkArgument(!query.requests().isEmpty());
    }

    protected Query(
            AbstractWatchListener listener,
            ListenableFuture<List<Operation.ProtocolResponse<?>>> future) {
        super(future);
        this.listener = listener;
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public void run() {
        if (isDone() && listener.isRunning()) {
            try {
                call();
            } catch (Exception e) {
                listener.stopping(listener.state());
                return;
            }
        }
    }
    
    @Override
    public Optional<Operation.Error> call() throws Exception {
        Optional<Operation.Error> error = null;
        for (Operation.ProtocolResponse<?> response: get()) {
            error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
        }
        assert (error != null);
        return error;
    }
}