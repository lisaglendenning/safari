package edu.uw.zookeeper.orchestra.frontend;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutorActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

public class BackendSessionExecutor extends ExecutorActor<BackendSessionExecutor.BackendRequestFuture> implements TaskExecutor<Pair<OperationFuture<?>, ShardedRequestMessage<?>>, ShardedResponseMessage<?>> {

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
    
    protected final Logger logger;
    protected final long frontend;
    protected final Identifier ensemble;
    protected final Session session;
    protected final ClientPeerConnection<?> connection;
    protected final Publisher publisher;
    protected final Executor executor;
    protected final LinkedQueue<BackendRequestFuture> mailbox;
    // not thread safe
    protected LinkedIterator<BackendRequestFuture> pending;
    // not thread safe
    protected LinkedIterator<BackendRequestFuture> finger;

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
        this.finger = mailbox.iterator();
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
            send(task);
        } catch (Exception e) {
            task.setException(e);
        }
        return task;
    }

    @Override
    public synchronized void send(BackendRequestFuture request) {
        if (state() == State.TERMINATED) {
            throw new RejectedExecutionException(State.TERMINATED.toString());
        } 
        // we won't get the response before putting the request in the queue
        // because we are synchronized
        if (request.state() == OperationFuture.State.WAITING) {
            try {
                request.call();
            } catch (Exception e) {
                throw new RejectedExecutionException(e);
            }
        }
        super.send(request);
        if (! pending.hasNext()) {
            pending = mailbox.iterator();
        }
    }
    
    @Subscribe
    public synchronized void handleResponse(ShardedResponseMessage<?> message) {
        if (state() == State.TERMINATED) {
            return;
        }
        int xid = message.getXid();
        if (xid == OpCodeXid.NOTIFICATION.getXid()) {
            while (pending.hasNext() && pending.peekNext().isDone()) {
                pending.next();
            }
            pending.add(new BackendNotification(message));
        } else {
            BackendResponseTask task = null;
            OperationFuture<ShardedResponseMessage<?>> next;
            while ((next = pending.peekNext()) != null) {
                int nextXid = next.getXid();
                if (nextXid == OpCodeXid.NOTIFICATION.getXid()) {
                    pending.next();
                } else if (nextXid == xid) {
                    task = (BackendResponseTask) pending.next();
                    break;
                } else if (pending.peekNext().isDone()) {
                    pending.next();
                } else {
                    // FIXME
                    throw new AssertionError(String.format("%s %s",  message, pending));
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
        
        run();
    }
    
    @Subscribe
    public synchronized void handleTransition(Automaton.Transition<?> event) {
        if (state() == State.TERMINATED) {
            return;
        }
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            KeeperException.ConnectionLossException e = new KeeperException.ConnectionLossException();
            for (BackendRequestFuture next: mailbox) {
                if (! next.isDone()) {
                    if (next instanceof Promise) {
                        ((Promise<?>) next).setException(e);
                    }
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
    protected synchronized void doRun() throws ExecutionException {
        finger = mailbox.iterator();
        BackendRequestFuture next;
        while ((next = finger.peekNext()) != null) {
            if (! apply(next)) {
                break;
            }
        }
    }
    
    @Override
    protected synchronized boolean apply(BackendRequestFuture input) throws ExecutionException {
        if (state() != State.TERMINATED) {
            for (;;) {
                OperationFuture.State state = input.state();
                if (OperationFuture.State.PUBLISHED == state) {
                    finger.next();
                    finger.remove();
                    break;
                } else if ((OperationFuture.State.COMPLETE == state)
                        && (finger.hasPrevious() && (finger.peekPrevious().state().compareTo(OperationFuture.State.PUBLISHED) < 0))) {
                    // tasks can only publish when predecessors have published
                    break;
                } else if (input.call() == state) {
                    break;
                }
            }
            
            if (finger.peekNext() == input) {
                return false;
            }
        }
        return (state() != State.TERMINATED);
    }
    
    protected synchronized void doStop() {
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
            checkState(OpCodeXid.NOTIFICATION.getXid() == message.getXid());
            this.state = new AtomicReference<State>(state);
            this.message = message;
        }
        
        @Override
        public BackendSessionExecutor executor() {
            return BackendSessionExecutor.this;
        }
        
        @Override
        public int getXid() {
            return OpCodeXid.NOTIFICATION.getXid();
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
            return state();
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
        public int getXid() {
            return task().second().getXid();
        }

        @Override
        public State state() {
            return state.get();
        }
        
        @Override
        public State call() throws ExecutionException {
            switch (state()) {
            case WAITING:
            {
                if (state.compareAndSet(State.WAITING, State.SUBMITTING)) {
                    MessagePacket message = MessagePacket.of(
                            MessageSessionRequest.of(frontend, task().second()));
                    writeFuture = getConnection().write(message);
                    Futures.addCallback(writeFuture, this);
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
            return state();
        }
        
        @Override
        public boolean set(ShardedResponseMessage<?> result) {
            if (result.getXid() != task().second().getXid()) {
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
    }
}