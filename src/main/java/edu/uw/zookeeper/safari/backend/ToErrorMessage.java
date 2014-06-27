package edu.uw.zookeeper.safari.backend;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.peer.protocol.ShardedClientRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedErrorResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;

public final class ToErrorMessage extends ForwardingListenableFuture<ShardedServerResponseMessage<?>> implements Callable<Optional<ShardedResponseMessage<?>>>, Runnable {
    
    public static ListenableFuture<ShardedResponseMessage<?>> submit(
            ClientExecutor<? super ShardedClientRequestMessage<?>, ShardedServerResponseMessage<?>, ?> client,
            ShardedClientRequestMessage<?> request) {
        Promise<ShardedResponseMessage<?>> promise = SettableFuturePromise.create();
        ListenableFuture<ShardedServerResponseMessage<?>> response = client.submit(request);
        new ToErrorMessage(request, response, promise);
        return promise;
    }
    
    private final ShardedClientRequestMessage<?> request;
    private final ListenableFuture<ShardedServerResponseMessage<?>> future;
    private final CallablePromiseTask<ToErrorMessage, ShardedResponseMessage<?>> delegate;
    
    protected ToErrorMessage(
            ShardedClientRequestMessage<?> request,
            ListenableFuture<ShardedServerResponseMessage<?>> response,
            Promise<ShardedResponseMessage<?>> promise) {
        this.request = request;
        this.future = response;
        this.delegate = CallablePromiseTask.create(this, promise);
        
        addListener(this, SameThreadExecutor.getInstance());
        delegate.addListener(this, SameThreadExecutor.getInstance());
    }

    @Override
    public void run() {
        if (delegate.isDone()) {
            if (delegate.isCancelled()) {
                cancel(false);
            }
        } else {
            delegate.run();
        }
    }

    @Override
    public Optional<ShardedResponseMessage<?>> call() throws Exception {
        if (isDone()) {
            ShardedResponseMessage<?> result;
            try {
                result = get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof SafariException) {
                    result = ShardedErrorResponseMessage.valueOf(request.getShard(), request.xid(), (SafariException) e.getCause());
                } else {
                    throw e;
                }
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
            return Optional.<ShardedResponseMessage<?>>of(result);
        }
        return Optional.absent();
    }

    @Override
    protected ListenableFuture<ShardedServerResponseMessage<?>> delegate() {
        return future;
    }
}