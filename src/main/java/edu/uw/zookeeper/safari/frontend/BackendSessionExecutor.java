package edu.uw.zookeeper.safari.frontend;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import net.engio.mbassy.listener.Handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.collect.Queues;
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

public class BackendSessionExecutor extends ExecutedActor<BackendSessionExecutor.BackendRequestTask> 
        implements TaskExecutor<Pair<MessageSessionRequest, ? extends ListenableFuture<?>>, ShardedResponseMessage<?>> {

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
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final Identifier ensemble;
    protected final Session session;
    protected final ClientPeerConnection<?> connection;
    protected final FutureCallback<? super ShardedResponseMessage<?>> callback;
    protected final Executor executor;
    protected final Queue<BackendRequestTask> mailbox;
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
            Pair<MessageSessionRequest, ? extends ListenableFuture<?>> request) {
        BackendRequestTask task = new BackendRequestTask(request,
                LoggingPromise.create(logger, 
                        SettableFuturePromise.<ShardedResponseMessage<?>>create()));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Handler
    public void handleResponse(ShardedResponseMessage<?> message) {
        if (state() == State.TERMINATED) {
            return;
        }
        pending.send(message);
    }

    @Handler
    public void handleTransition(Automaton.Transition<?> event) {
        if (state() == State.TERMINATED) {
            return;
        }
        // FIXME
        if ((Connection.State.CONNECTION_CLOSING == event.to()) 
                || (Connection.State.CONNECTION_CLOSED == event.to())) {
            KeeperException.OperationTimeoutException e = new KeeperException.OperationTimeoutException();
            for (BackendRequestTask next: mailbox) {
                if (! next.isDone()) {
                    next.setException(e);
                }
            }
            stop();
        }
    }
    
    @Override
    protected Queue<BackendRequestTask> mailbox() {
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
    protected boolean apply(BackendRequestTask input) {
        if (! input.isDone()) {
            // add to pending before write
            if (!pending.enqueue(input)) {
                return false;
            }
            MessagePacket<MessageSessionRequest> message = MessagePacket.of(input.task().first());
            ListenableFuture<MessagePacket<MessageSessionRequest>> future = getConnection().write(message);
            Futures.addCallback(future, input, SAME_THREAD_EXECUTOR);
        }
        return (state() != State.TERMINATED);
    }

    @Override
    protected void doStop() {
        // FIXME
        BackendRequestTask next;
        while ((next = mailbox.poll()) != null) {
            next.cancel(true);
        }
        pending.stop();
        completed.stop();
    }

    protected static class BackendRequestTask 
            extends PromiseTask<Pair<MessageSessionRequest, ? extends ListenableFuture<?>>, ShardedResponseMessage<?>> 
            implements Operation.RequestId, FutureCallback<Object> {

        public BackendRequestTask(
                Pair<MessageSessionRequest, ? extends ListenableFuture<?>> task,
                Promise<ShardedResponseMessage<?>> promise) {
            super(task, promise);
        }
        
        @Override
        public boolean set(ShardedResponseMessage<?> response) {
            assert (response.xid() == xid());
            return super.set(response);
        }
        
        @Override
        public int xid() {
            return task.first().getValue().xid();
        }

        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }

        @Override
        protected Objects.ToStringHelper toString(Objects.ToStringHelper toString) {
            return toString.add("task", task.first())
                    .add("future", delegate);
        }
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
            return message.xid();
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
            extends PromiseTask<BackendRequestTask, ShardedResponseMessage<?>> 
            implements BackendRequestFuture, Runnable {

        public BackendResponseTask(
                BackendRequestTask task,
                Promise<ShardedResponseMessage<?>> promise) {
            super(task, promise);
            
            task.addListener(this, SAME_THREAD_EXECUTOR);
            task.task().second().addListener(this, SAME_THREAD_EXECUTOR);
        }
        
        @Override
        public int xid() {
            return task.xid();
        }

        @Override
        public void run() {
            if (! isDone()) {
                if (task.isDone() && task.task().second().isDone()) {
                    if (task.isCancelled()) {
                        cancel(true);
                    } else {
                        try {
                            set(task.get());
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            setException(e);
                        }
                    }
                }
            }
        }
    }
    
    protected class PendingProcessor extends AbstractActor<ShardedResponseMessage<?>> {

        protected final Queue<BackendRequestTask> mailbox;

        public PendingProcessor() {
            this.mailbox = Queues.newConcurrentLinkedQueue();
        }
        
        public boolean enqueue(BackendRequestTask task) {
            mailbox.add(task);
            if (state() == State.TERMINATED) {
                mailbox.remove(task);
                return false;
            } else {
                return true;
            }
        }
        
        @Override
        protected boolean doSend(ShardedResponseMessage<?> message) {
            int xid = message.xid();
            if (xid == OpCodeXid.NOTIFICATION.xid()) {
                return completed.send(new BackendNotification(message));
            } else {
                BackendRequestTask task = mailbox.peek();
                assert(task != null);
                if (! task.set(message)) {
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
            BackendRequestTask next;
            while ((next = mailbox.peek()) != null) {
                if (next.isDone()) {
                    mailbox.remove(next);
                    completed.send(new BackendResponseTask(next,
                            LoggingPromise.create(logger, 
                                    SettableFuturePromise.<ShardedResponseMessage<?>>create())));
                } else {
                    break;
                }
            }
        }

        @Override
        protected void runExit() {
            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                BackendRequestTask next = mailbox.peek();
                if ((next != null) && (next.isDone())) {
                    schedule();
                }
            }
        }
        
        @Override
        protected void doStop() {
            BackendRequestTask next;
            while ((next = mailbox.poll()) != null) {
                next.cancel(true);
            }
        }

        @Override
        protected Logger logger() {
            return logger;
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
            this.mailbox = Queues.newConcurrentLinkedQueue();
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
                        next.addListener(this, SAME_THREAD_EXECUTOR);
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