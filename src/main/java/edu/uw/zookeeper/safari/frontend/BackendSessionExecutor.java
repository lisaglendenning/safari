package edu.uw.zookeeper.safari.frontend;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.collect.Queues;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class BackendSessionExecutor extends ExecutedActor<BackendSessionExecutor.BackendResponseTask> implements TaskExecutor<Pair<? extends ListenableFuture<?>, MessageSessionRequest>, ShardedResponseMessage<?>> {

    public static interface BackendRequestFuture extends ListenableFuture<ShardedResponseMessage<?>>, Operation.RequestId {
    }
    
    public static BackendSessionExecutor create(
            Identifier ensemble,
            Session session,
            ClientPeerConnection<?> connection,
            FutureCallback<? super ShardedResponseMessage<?>> callback,
            Executor executor) {
        return new BackendSessionExecutor(
                ensemble, session, connection, callback, executor);
    }
    
    protected static final Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final Identifier ensemble;
    protected final Session session;
    protected final ClientPeerConnection<?> connection;
    protected final FutureCallback<? super ShardedResponseMessage<?>> callback;
    protected final Executor executor;
    protected final Queue<BackendResponseTask> mailbox;
    protected final PendingProcessor pending;
    protected final CompletedProcessor completed;

    protected BackendSessionExecutor(
            Identifier ensemble,
            Session session,
            ClientPeerConnection<?> connection,
            FutureCallback<? super ShardedResponseMessage<?>> callback,
            Executor executor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.ensemble = ensemble;
        this.session = session;
        this.connection = connection;
        this.callback = callback;
        this.executor = executor;
        this.mailbox = Queues.newConcurrentLinkedQueue();
        this.pending = new PendingProcessor();
        this.completed = new CompletedProcessor();
    }
    
    public Identifier getEnsemble() {
        return ensemble;
    }
    
    public Session getSession() {
        return session;
    }
    
    public ClientPeerConnection<?> getConnection() {
        return connection;
    }
    
    @Override
    public ListenableFuture<ShardedResponseMessage<?>> submit(
            Pair<? extends ListenableFuture<?>, MessageSessionRequest> request) {
        BackendResponseTask task = new BackendResponseTask(request,
                LoggingPromise.create(logger, 
                        SettableFuturePromise.<ShardedResponseMessage<?>>create()),
                LoggingPromise.create(logger, SettableFuturePromise.<ShardedResponseMessage<?>>create()));
        try {
            if (! send(task)) {
                task.cancel(true);
            }
        } catch (Exception e) {
            task.setException(e);
        }
        return task.getResponse();
    }

    @Subscribe
    public void handleResponse(ShardedResponseMessage<?> message) {
        if (state() == State.TERMINATED) {
            return;
        }
        pending.send(message);
    }
    
    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (state() == State.TERMINATED) {
            return;
        }
        // FIXME
        if ((Connection.State.CONNECTION_CLOSING == event.to()) 
                || (Connection.State.CONNECTION_CLOSED == event.to())) {
            KeeperException.OperationTimeoutException e = new KeeperException.OperationTimeoutException();
            for (BackendRequestFuture next: mailbox) {
                if (! next.isDone()) {
                    ((Promise<?>) next).setException(e);
                }
            }
            stop();
        }
    }
    
    @Override
    protected Queue<BackendResponseTask> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    @Override
    protected boolean apply(BackendResponseTask input) {
        // add to pending before write
        if (!pending.enqueue(input)) {
            return false;
        }
        MessagePacket<MessageSessionRequest> message = MessagePacket.of(input.task().second());
        ListenableFuture<MessagePacket<MessageSessionRequest>> future = getConnection().write(message);
        Futures.addCallback(future, input, sameThreadExecutor);
        return (state() != State.TERMINATED);
    }

    @Override
    protected void doStop() {
        // FIXME
        BackendRequestFuture next;
        while ((next = mailbox.poll()) != null) {
            next.cancel(true);
        }
        pending.stop();
        completed.stop();
    }

    protected static class BackendNotification implements BackendRequestFuture {

        protected final ShardedResponseMessage<?> message;

        public BackendNotification(
                ShardedResponseMessage<?> message) {
            assert(OpCodeXid.NOTIFICATION.xid() == message.xid());
            this.message = message;
        }
        
        @Override
        public int xid() {
            return OpCodeXid.NOTIFICATION.xid();
        }
        
        @Override
        public void addListener(Runnable listener, Executor executor) {
            executor.execute(listener);
        }

        @Override
        public boolean cancel(boolean mayInterrupt) {
            return false;
        }

        @Override
        public ShardedResponseMessage<?> get() {
            return message;
        }

        @Override
        public ShardedResponseMessage<?> get(long time, TimeUnit unit) {
            return message;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("message", message)
                    .toString();
        }
    }
    
    protected static class BackendResponseTask 
            extends PromiseTask<Pair<? extends ListenableFuture<?>, MessageSessionRequest>, ShardedResponseMessage<?>> 
            implements FutureCallback<MessagePacket<MessageSessionRequest>>, BackendRequestFuture, Runnable {

        protected final Promise<ShardedResponseMessage<?>> response;

        public BackendResponseTask(
                Pair<? extends ListenableFuture<?>, MessageSessionRequest> task,
                Promise<ShardedResponseMessage<?>> promise,
                Promise<ShardedResponseMessage<?>> response) {
            super(task, promise);
            this.response = response;
            
            this.response.addListener(this, sameThreadExecutor);
            this.task.first().addListener(this, sameThreadExecutor);
        }
        
        public ListenableFuture<ShardedResponseMessage<?>> getResponse() {
            return response;
        }
        
        protected boolean setResponse(ShardedResponseMessage<?> response) {
            assert(response.xid() == xid());
            return this.response.set(response);
        }
        
        @Override
        public void run() {
            if (! isDone()) {
                if (task.first().isDone() && response.isDone()) {
                    if (response.isCancelled()) {
                        cancel(true);
                    } else {
                        try {
                            set(response.get());
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            setException(e);
                        }
                    }
                }
            }
        }
        
        @Override
        public int xid() {
            return task.second().getValue().xid();
        }

        @Override
        public void onSuccess(MessagePacket<MessageSessionRequest> result) {
        }

        @Override
        public void onFailure(Throwable t) {
            response.setException(t);
        }

        @Override
        protected Objects.ToStringHelper toString(Objects.ToStringHelper toString) {
            return toString.add("task", task.second())
                    .add("response", response).add("future", delegate);
        }
    }
    
    protected class PendingProcessor extends AbstractActor<ShardedResponseMessage<?>> {

        protected final Queue<BackendResponseTask> mailbox;

        public PendingProcessor() {
            this.mailbox = Queues.newConcurrentLinkedQueue();
        }
        
        public boolean enqueue(BackendResponseTask task) {
            mailbox.add(task);
            if (state() == State.TERMINATED) {
                mailbox.remove(task);
                return false;
            } else {
                return true;
            }
        }
        
        @Override
        protected Logger logger() {
            return logger;
        }

        @Override
        protected boolean doSend(ShardedResponseMessage<?> message) {
            int xid = message.xid();
            if (xid == OpCodeXid.NOTIFICATION.xid()) {
                return completed.send(new BackendNotification(message));
            } else {
                BackendResponseTask task = mailbox.peek();
                assert(task != null);
                if (! task.setResponse(message)) {
                    throw new AssertionError(String.valueOf(task));
                }
                if (!schedule() && (state() == State.TERMINATED)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        protected void doRun() {
            BackendResponseTask next;
            while ((next = mailbox.peek()) != null) {
                if (next.getResponse().isDone()) {
                    mailbox.remove(next);
                    completed.send(next);
                } else {
                    break;
                }
            }
        }

        @Override
        protected void runExit() {
            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                BackendResponseTask next = mailbox.peek();
                if ((next != null) && (next.getResponse().isDone())) {
                    schedule();
                }
            }
        }
        
        @Override
        protected void doStop() {
            BackendResponseTask next;
            while ((next = mailbox.poll()) != null) {
                next.cancel(true);
            }
        }
    }

    // hang on to completed requests to ensure that notification
    // messages are sent in order
    // because frontend requests may be waiting on multiple backend requests
    protected class CompletedProcessor extends ExecutedActor<BackendRequestFuture> {

        protected final Queue<BackendRequestFuture> mailbox;
        // not thread-safe
        protected final Set<BackendRequestFuture> futures;
        
        public CompletedProcessor() {
            mailbox = Queues.newConcurrentLinkedQueue();
            this.futures = Collections.newSetFromMap(
                    new WeakHashMap<BackendRequestFuture, Boolean>());
        }
        
        @Override
        protected Executor executor() {
            return executor;
        }

        @Override
        protected Logger logger() {
            return logger;
        }

        @Override
        protected Queue<BackendRequestFuture> mailbox() {
            return mailbox;
        }
        
        @Override
        protected void doRun() {
            BackendRequestFuture next;
            while ((next = mailbox.peek()) != null) {
                if (next.isDone()) {
                    mailbox.remove(next);
                    if (! apply(next)) {
                        break;
                    }
                } else {
                    if (futures.add(next)) {
                        next.addListener(this, sameThreadExecutor);
                    }
                    break;
                }
            }
        }

        @Override
        protected boolean apply(BackendRequestFuture input) {
            try {
                callback.onSuccess(input.get());
            } catch (ExecutionException e) {
                // FIXME 
                throw new AssertionError(e);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            return (state() != State.TERMINATED);
        }

        @Override
        protected void runExit() {
            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                BackendRequestFuture next = mailbox.peek();
                if ((next != null) && (next.isDone())) {
                    schedule();
                }
            }
        }
        
        @Override
        protected void doStop() {
            BackendRequestFuture next;
            while ((next = mailbox.poll()) != null) {
                next.cancel(true);
            }
        }
    }
}