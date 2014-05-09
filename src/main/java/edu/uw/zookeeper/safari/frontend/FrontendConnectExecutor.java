package edu.uw.zookeeper.safari.frontend;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.server.SessionManager;
import edu.uw.zookeeper.server.SimpleConnectExecutor;

public class FrontendConnectExecutor implements TaskExecutor<ConnectMessage.Request, ConnectMessage.Response> {

    public static FrontendConnectExecutor defaults(
            FrontendSessionExecutor.Factory factory,
            ConcurrentMap<Long, FrontendSessionExecutor> executors,
            SessionManager sessions,
            ZxidReference lastZxid) {
        return create(
                factory,
                executors,
                SimpleConnectExecutor.defaults(executors, sessions, lastZxid));
    }
    
    public static FrontendConnectExecutor create(
            FrontendSessionExecutor.Factory factory,
            ConcurrentMap<Long, FrontendSessionExecutor> executors,
            SimpleConnectExecutor<FrontendSessionExecutor> delegate) {
        return new FrontendConnectExecutor(factory, executors, delegate);
    }
    
    protected final FrontendSessionExecutor.Factory factory;
    protected final ConcurrentMap<Long, FrontendSessionExecutor> executors;
    protected final SimpleConnectExecutor<FrontendSessionExecutor> delegate;
    
    protected FrontendConnectExecutor(
            FrontendSessionExecutor.Factory factory,
            ConcurrentMap<Long, FrontendSessionExecutor> executors,
            SimpleConnectExecutor<FrontendSessionExecutor> delegate) {
        this.factory = factory;
        this.executors = executors;
        this.delegate = delegate;
    }

    @Override
    public ListenableFuture<ConnectMessage.Response> submit(ConnectMessage.Request request) {
        ListenableFuture<ConnectMessage.Response> result = delegate.submit(request);
        assert (result.isDone());
        try {
            ConnectMessage.Response response = result.get();
            if (response instanceof ConnectMessage.Response.Valid) {
                Callback callback = new Callback(response,
                        SettableFuturePromise.<ConnectMessage.Response>create());
                callback.run();
                return callback;
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KeeperException.UnknownSessionException) {
                // may not be expired in the backend
                FrontendSessionExecutor executor = factory.get(request.toSession());
                FrontendSessionExecutor existing = executors.putIfAbsent(Long.valueOf(request.getSessionId()), executor);
                assert (existing == null);
                return submit(request);
            }
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        return result;
    }

    protected class Callback extends PromiseTask<ConnectMessage.Response, ConnectMessage.Response> implements FutureCallback<Set<ClientPeerConnectionExecutor>>, Runnable {

        public Callback(
                ConnectMessage.Response task, 
                Promise<ConnectMessage.Response> promise) {
            super(task, promise);
        }

        @Override
        public void run() {
            FrontendSessionExecutor executor = executors.get(Long.valueOf(task.getSessionId()));
            if (executor != null) {
                Futures.addCallback(executor.backends().connect(), 
                        this, SameThreadExecutor.getInstance());
            } else {
                // FIXME
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void onSuccess(Set<ClientPeerConnectionExecutor> result) {
            set(task);
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}
