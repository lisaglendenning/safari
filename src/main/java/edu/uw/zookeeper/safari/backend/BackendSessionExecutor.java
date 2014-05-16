package edu.uw.zookeeper.safari.backend;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.peer.protocol.ShardedClientRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedErrorResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;

public class BackendSessionExecutor extends AbstractPair<Long, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> implements TaskExecutor<ShardedClientRequestMessage<?>, ShardedResponseMessage<?>> {

    public static BackendSessionExecutor create(
            Long session,
            ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>> client) {
        return new BackendSessionExecutor(session, client);
    }
    
    protected BackendSessionExecutor(
            Long session,
            ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>> client) {
        super(session, client);
    }
    
    public Long session() {
        return first;
    }
    
    public ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>> client() {
        return second;
    }
    
    @Override
    public ListenableFuture<ShardedResponseMessage<?>> submit(
            ShardedClientRequestMessage<?> request) {
        return RequestListener.create(
                request, client().submit(request), 
                SettableFuturePromise.<ShardedResponseMessage<?>>create());
    }

    protected static class RequestListener extends PromiseTask<ShardedClientRequestMessage<?>, ShardedResponseMessage<?>> implements Callable<Optional<ShardedResponseMessage<?>>> {
        
        public static RequestListener create(
                ShardedClientRequestMessage<?> request,
                ListenableFuture<ShardedServerResponseMessage<?>> response,
                Promise<ShardedResponseMessage<?>> promise) {
            RequestListener listener = new RequestListener(request, response, promise);
            response.addListener(CallablePromiseTask.create(listener, listener), SameThreadExecutor.getInstance());
            return listener;
        }
        
        private final ListenableFuture<ShardedServerResponseMessage<?>> response;
        
        public RequestListener(
                ShardedClientRequestMessage<?> request,
                ListenableFuture<ShardedServerResponseMessage<?>> response,
                Promise<ShardedResponseMessage<?>> promise) {
            super(request, promise);
            this.response = response;
        }

        @Override
        public Optional<ShardedResponseMessage<?>> call() throws Exception {
            if (response.isDone()) {
                ShardedResponseMessage<?> result;
                try {
                    result = response.get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof SafariException) {
                        result = ShardedErrorResponseMessage.valueOf(task().getShard(), task().xid(), (SafariException) e.getCause());
                    } else {
                        setException(e.getCause());
                        throw Throwables.propagate(e.getCause());
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return Optional.<ShardedResponseMessage<?>>of(result);
            }
            return Optional.absent();
        }
    }
}
