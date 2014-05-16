package edu.uw.zookeeper.safari.backend;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.safari.storage.StorageSchema;
import edu.uw.zookeeper.safari.storage.StorageZNode;

public class RegisterSessionTask extends PromiseTask<Pair<Long, ? extends ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>, Void> implements Runnable, FutureCallback<Object>, Connection.Listener<Operation.Response> {
    
    public static RegisterSessionTask create(
            Materializer<StorageZNode<?>,?> materializer,
            Pair<Long, ? extends ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> task,
            Promise<Void> promise) {
        return new RegisterSessionTask(materializer, task, promise);
    }

    // magic constant xid
    protected static final int XID = 0;

    private final Materializer<StorageZNode<?>,?> materializer;
    private Optional<? extends ListenableFuture<? extends Message.ClientRequest<?>>> registered = Optional.absent();
    
    public RegisterSessionTask(
            Materializer<StorageZNode<?>,?> materializer,
            Pair<Long, ? extends ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> task,
            Promise<Void> promise) {
        super(task, promise);
        this.materializer = materializer;
        task().second().addListener(this, SameThreadExecutor.getInstance());
        addListener(this, SameThreadExecutor.getInstance());
    }
    
    @Override
    public synchronized void run() {
        if (isDone()) {
            task().second().task().second().unsubscribe(this);
        } else if (task().second().isDone()) {
            if (!registered.isPresent()) {
                try {
                    task().second().task().second().subscribe(this);
                    final StorageSchema.Safari.Sessions.Session.Data value = StorageSchema.Safari.Sessions.Session.Data.valueOf(task().second().get().getSessionId(), task().second().get().getPasswd());
                    final ZNodePath path = StorageSchema.Safari.Sessions.Session.pathOf(task().first());
                    final Message.ClientRequest<?> request = ProtocolRequestMessage.of(XID, materializer.create(path, value).get().build());
                    this.registered = Optional.of(task().second().task().second().write(request));
                    Futures.addCallback(registered.get(), this, SameThreadExecutor.getInstance());
                } catch (ExecutionException e) {
                    setException(e.getCause());
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }

    @Override
    public void handleConnectionState(Automaton.Transition<edu.uw.zookeeper.net.Connection.State> state) {
        if (Connection.State.CONNECTION_CLOSED == state.to()) {
            onFailure(new KeeperException.ConnectionLossException());
        }
    }

    @Override
    public void handleConnectionRead(Operation.Response message) {
        assert (((Operation.ProtocolResponse<?>) message).xid() == XID);
        try {
            Operations.unlessError(((Operation.ProtocolResponse<?>) message).record());
        } catch (KeeperException e) {
            setException(e);
            return;
        }
        set(null);
    }

    @Override
    public void onSuccess(Object result) {
    }

    @Override
    public void onFailure(Throwable t) {
        setException(t);
    }
}