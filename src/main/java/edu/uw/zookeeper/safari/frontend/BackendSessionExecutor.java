package edu.uw.zookeeper.safari.frontend;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.LinkedIterator;
import edu.uw.zookeeper.safari.common.LinkedQueue;
import edu.uw.zookeeper.safari.common.OperationFuture;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class BackendSessionExecutor extends ExecutedActor<BackendSessionExecutor.BackendRequestFuture> implements TaskExecutor<Pair<OperationFuture<?>, ShardedRequestMessage<?>>, ShardedResponseMessage<?>> {

    public static interface BackendRequestFuture extends OperationFuture<ShardedResponseMessage<?>> {
        BackendSessionExecutor executor();

        @Override
        State call() throws ExecutionException;
    }
    
    public static BackendSessionExecutor create(
            long frontend,
            Identifier ensemble,
            Session session,
            ClientPeerConnection<?> connection,
            Publisher publisher,
            Executor executor) {
        return new BackendSessionExecutor(
                frontend, ensemble, session, connection, publisher, executor);
    }
    
    protected static final Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final long frontend;
    protected final Identifier ensemble;
    protected final Session session;
    protected final ClientPeerConnection<?> connection;
    protected final Publisher publisher;
    protected final Executor executor;
    protected final LinkedQueue<BackendRequestFuture> mailbox;
    // not thread safe
    protected volatile LinkedIterator<BackendRequestFuture> pending;
    protected volatile LinkedIterator<BackendRequestFuture> finger;

    public BackendSessionExecutor(
            long frontend,
            Identifier ensemble,
            Session session,
            ClientPeerConnection<?> connection,
            Publisher publisher,
            Executor executor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.frontend = frontend;
        this.ensemble = ensemble;
        this.session = session;
        this.connection = connection;
        this.publisher = publisher;
        this.executor = executor;
        this.mailbox = LinkedQueue.create();
        this.pending = mailbox.iterator();
        this.finger = null;
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
    public BackendRequestFuture submit(
            Pair<OperationFuture<?>, ShardedRequestMessage<?>> request) {
        BackendResponseTask task = new BackendResponseTask(request);
        try {
            if (! send(task)) {
                task.cancel(true);
            }
        } catch (Exception e) {
            task.setException(e);
        }
        return task;
    }

    @Override
    public boolean send(BackendRequestFuture message) {
        logger.debug("Submitting: {}", message);
        synchronized (mailbox) {
            synchronized (message) {
                // ensure that queue order is same as submit order...
                if (! super.send(message)) {
                    return false;
                }
                
                if (message.state() == OperationFuture.State.WAITING) {
                    try {
                        message.call();
                    } catch (Exception e) {
                        mailbox.remove(message);
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    @Subscribe
    public void handleResponse(ShardedResponseMessage<?> message) {
        if (state() == State.TERMINATED) {
            return;
        }
        logger.debug("{}", message);
        synchronized (mailbox) {
            if (! pending.hasNext()) {
                pending = mailbox.iterator();
            }
            logger.trace("{}", pending);
            int xid = message.xid();
            if (xid == OpCodeXid.NOTIFICATION.xid()) {
                while (pending.hasNext() && pending.peekNext().isDone()) {
                    pending.next();
                }
                pending.add(new BackendNotification(message));
            } else {
                BackendResponseTask task = null;
                OperationFuture<ShardedResponseMessage<?>> next;
                while ((next = pending.peekNext()) != null) {
                    int nextXid = next.xid();
                    if (nextXid == OpCodeXid.NOTIFICATION.xid()) {
                        pending.next();
                    } else if (nextXid == xid) {
                        task = (BackendResponseTask) pending.next();
                        break;
                    } else if (pending.peekNext().isDone()) {
                        pending.next();
                    } else {
                        // FIXME
                        throw new AssertionError(String.format("%s != %s",  message, pending));
                    }
                }
                if (task == null) {
                    // FIXME
                    throw new AssertionError();
                } else {
                    if (! task.set(message)) {
                        // FIXME
                        throw new AssertionError();
                    }
                }
            }
            logger.trace("{}", pending);
        }
        
        run();
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
    
    protected LinkedQueue<BackendRequestFuture> mailbox() {
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
    protected void doRun() throws ExecutionException {
        finger = mailbox.iterator();
        BackendRequestFuture next;
        while ((next = finger.peekNext()) != null) {
            if (!apply(next) || (finger.peekNext() == next)) {
                break;
            }
        }
        finger = null;
    }
    
    @Override
    protected boolean apply(BackendRequestFuture input) throws ExecutionException {
        synchronized (input) {
            for (;;) {
                OperationFuture.State state = input.state();
                if (OperationFuture.State.PUBLISHED == state) {
                    assert (finger.peekNext() != pending.peekNext());
                    finger.next();
                    finger.remove();
                    break;
                } else if ((OperationFuture.State.COMPLETE == state)
                        && (finger.hasPrevious() && (finger.peekPrevious().state().compareTo(state) <= 0))) {
                    // tasks can only publish when predecessors have published
                    break;
                } else if (input.call() == state) {
                    break;
                }
            }
        }
        return (state() != State.TERMINATED);
    }
    
    @Override
    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            // TODO ??
            //BackendRequestFuture head = mailbox.peek();
            //if ((head != null) && head.isDone()) {
                // TODO ??
            //}
        }
    }
    
    protected void doStop() {
        // FIXME
        BackendRequestFuture next;
        while ((next = mailbox.poll()) != null) {
            next.cancel(true);
        }
    }

    protected class BackendNotification implements BackendRequestFuture {

        protected final AtomicReference<State> state;
        protected final ShardedResponseMessage<?> message;

        public BackendNotification(
                ShardedResponseMessage<?> message) {
            this(State.COMPLETE, message);
        }
        public BackendNotification(
                State state,
                ShardedResponseMessage<?> message) {
            checkState(OpCodeXid.NOTIFICATION.xid() == message.xid());
            this.state = new AtomicReference<State>(state);
            this.message = message;
        }
        
        @Override
        public BackendSessionExecutor executor() {
            return BackendSessionExecutor.this;
        }
        
        @Override
        public int xid() {
            return OpCodeXid.NOTIFICATION.xid();
        }
        @Override
        public State state() {
            return state.get();
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
        public State call() { 
            logger.entry(this);
            
            State state = state();
            switch (state) {
            case WAITING:
            case SUBMITTING:
            {
                this.state.compareAndSet(state, State.COMPLETE);
                break;
            }
            case COMPLETE:
            {
                if (this.state.compareAndSet(State.COMPLETE, State.PUBLISHED)) {
                    publisher.post(get());
                }
                break;
            }
            default:
                break;
            }
            return logger.exit(state());
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("state", state())
                    .add("message", message)
                    .toString();
        }
    }
    
    protected class BackendResponseTask 
            extends PromiseTask<Pair<OperationFuture<?>, ShardedRequestMessage<?>>, ShardedResponseMessage<?>> 
            implements FutureCallback<MessagePacket>, BackendRequestFuture {

        protected final AtomicReference<State> state;
        protected volatile ListenableFuture<MessagePacket> writeFuture;

        public BackendResponseTask(
                Pair<OperationFuture<?>, ShardedRequestMessage<?>> task) {
            this(State.WAITING, 
                    task, 
                    LoggingPromise.create(logger, 
                            SettableFuturePromise.<ShardedResponseMessage<?>>create()));
        }
        
        public BackendResponseTask(
                State state,
                Pair<OperationFuture<?>, ShardedRequestMessage<?>> task,
                Promise<ShardedResponseMessage<?>> delegate) {
            super(task, delegate);
            this.state = new AtomicReference<State>(state);
            this.writeFuture = null;
        }

        @Override
        public BackendSessionExecutor executor() {
            return BackendSessionExecutor.this;
        }
        
        @Override
        public int xid() {
            return task().second().xid();
        }

        @Override
        public State state() {
            return state.get();
        }
        
        @Override
        public State call() throws ExecutionException {
            logger.entry(this);
            
            switch (state()) {
            case WAITING:
            {
                if (state.compareAndSet(State.WAITING, State.SUBMITTING)) {
                    MessagePacket message = MessagePacket.of(
                            MessageSessionRequest.of(frontend, task().second()));
                    writeFuture = getConnection().write(message);
                    Futures.addCallback(writeFuture, this, sameThreadExecutor);
                }
                break;
            }
            case COMPLETE:
            {
                assert (isDone());
                Futures.get(this, ExecutionException.class);
                if (task().first().state() == State.PUBLISHED) {
                    this.state.compareAndSet(State.COMPLETE, State.PUBLISHED);
                }
                break;
            }
            default:
                break;
            }
            
            return logger.exit(state());
        }
        
        @Override
        public boolean set(ShardedResponseMessage<?> result) {
            if (result.xid() != xid()) {
                throw new IllegalArgumentException(result.toString());
            }
            boolean set = super.set(result);
            if (set) {
                state.compareAndSet(State.SUBMITTING, State.COMPLETE);
            }
            return set;
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancel = super.cancel(mayInterruptIfRunning);
            if (cancel) {
                if (writeFuture != null) {
                    writeFuture.cancel(mayInterruptIfRunning);
                }
                state.compareAndSet(State.WAITING, State.COMPLETE);
                state.compareAndSet(State.SUBMITTING, State.COMPLETE);
                run();
            }
            return cancel;
        }
        
        @Override
        public boolean setException(Throwable t) {
            boolean setException = super.setException(t);
            if (setException) {
                state.compareAndSet(State.WAITING, State.COMPLETE);
                state.compareAndSet(State.SUBMITTING, State.COMPLETE);
                run();
            }
            return setException;
        }

        @Override
        public void onSuccess(MessagePacket result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("state", state())
                    .add("task", task().second())
                    .add("future", delegate())
                    .toString();
        }
    }
}