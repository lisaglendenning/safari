package edu.uw.zookeeper.safari.backend;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.client.AbstractConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.LinkedIterator;
import edu.uw.zookeeper.safari.common.LinkedQueue;
import edu.uw.zookeeper.safari.common.OperationFuture;
import edu.uw.zookeeper.safari.peer.protocol.ShardedOperation;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class ShardedClientExecutor<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends AbstractConnectionClientExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>, ShardedClientExecutor<C>.ShardedRequestFuture<?>, C, Object> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ShardedClientExecutor<C> newInstance(
            FutureCallback<ShardedResponseMessage<?>> callback,
            Function<ZNodeLabel.Path, Identifier> lookup,
            CachedFunction<Identifier, OperationPrefixTranslator> translator,
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                callback,
                lookup,
                translator,
                ConnectTask.create(connection, request),
                connection,
                TimeValue.milliseconds(request.getTimeOut()),
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ShardedClientExecutor<C> newInstance(
            FutureCallback<ShardedResponseMessage<?>> callback,
            Function<ZNodeLabel.Path, Identifier> lookup,
            CachedFunction<Identifier, OperationPrefixTranslator> translator,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new ShardedClientExecutor<C>(
                callback,
                lookup,
                translator,
                session, 
                connection,
                timeOut,
                executor);
    }
    
    protected static Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

    protected final FutureCallback<ShardedResponseMessage<?>> callback;
    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final CachedFunction<Identifier, OperationPrefixTranslator> translator;
    protected final LinkedQueue<ShardedRequestFuture<?>> mailbox;
    // not thread safe
    protected volatile LinkedIterator<ShardedRequestFuture<?>> pending;
    protected volatile LinkedIterator<ShardedRequestFuture<?>> finger;
    
    protected ShardedClientExecutor(
            FutureCallback<ShardedResponseMessage<?>> callback,
            Function<ZNodeLabel.Path, Identifier> lookup,
            CachedFunction<Identifier, OperationPrefixTranslator> translator,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(session, connection, timeOut, executor);
        this.callback = callback;
        this.lookup = lookup;
        this.translator = translator;
        this.mailbox = LinkedQueue.create();
        this.pending = mailbox.iterator();
        this.finger = null;
    }

    public Message.ClientRequest<?> shard(Identifier shard, Message.ClientRequest<?> unsharded) throws Exception {
        ListenableFuture<OperationPrefixTranslator> future = translator.apply(shard);
        if (future.isDone()) {
            Records.Request record = unsharded.record();
            Records.Request translated = future.get().apply(record);
            Message.ClientRequest<?> sharded;
            if (translated == record) {
                sharded = unsharded;
            } else {
                sharded = ProtocolRequestMessage.of(
                        unsharded.xid(), translated);
            }
            return sharded;
        } else {
            return null;
        }
    }
    
    public Message.ServerResponse<?> unshard(Identifier shard, Message.ServerResponse<?> sharded) throws Exception {
        ListenableFuture<OperationPrefixTranslator> future = translator.apply(shard);
        if (future.isDone()) {
            Records.Response record = sharded.record();
            Records.Response translated = future.get().apply(record);
            Message.ServerResponse<?> unsharded;
            if (translated == record) {
                unsharded = sharded;
            } else {
                unsharded = ProtocolResponseMessage.of(
                        sharded.xid(), sharded.zxid(), translated);
            }
            return unsharded;
        } else {
            return null;
        }
    }

    @Override
    public ListenableFuture<ShardedResponseMessage<?>> submit(
            ShardedRequestMessage<?> request, Promise<ShardedResponseMessage<?>> promise) {
        ShardedRequestTask task = new ShardedRequestTask(request);
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    public boolean send(ShardedRequestFuture<?> message) {
        checkArgument(message.xid() != OpCodeXid.PING.xid());
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

    @SuppressWarnings("unchecked")
    @Override
    @Subscribe
    public void handleResponse(Message.ServerResponse<?> message) {
        super.handleResponse(message);

        if (state() == State.TERMINATED) {
            return;
        }
        logger.debug("{}", message);
        synchronized (mailbox) {
            if (! pending.hasNext()) {
                pending = mailbox.iterator();
            }
            logger.debug("{}", pending);
    
            int xid = message.xid();
            if (xid == OpCodeXid.NOTIFICATION.xid()) {
                while (pending.hasNext() && pending.peekNext().isDone()) {
                    pending.next();
                }
                pending.add(new NotificationTask((Message.ServerResponse<IWatcherEvent>) message));
            } else if (xid != OpCodeXid.PING.xid()) {
                ShardedRequestTask task = null;
                ShardedRequestFuture<?> next;
                while ((next = pending.peekNext()) != null) {
                    int nextXid = next.xid();
                    if (nextXid == OpCodeXid.NOTIFICATION.xid()) {
                        pending.next();
                    } else if (nextXid == xid) {
                        task = (ShardedRequestTask) pending.next();
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
                    if (! task.response().set(message)) {
                        // FIXME
                        throw new AssertionError();
                    }
                }
            }
        }
        
        run();
    }
    
    @Override
    public void onSuccess(Object result) {
    }

    @Override
    protected LinkedQueue<ShardedRequestFuture<?>> mailbox() {
        return mailbox;
    }

    @Override
    protected void doRun() throws Exception {
        finger = mailbox.iterator();
        ShardedRequestFuture<?> next;
        while ((next = finger.peekNext()) != null) {
            if (!apply(next) || (finger.peekNext() == next)) {
                break;
            }
        }
        finger = null;
    }
    
    @Override
    protected boolean apply(ShardedRequestFuture<?> input) throws Exception {
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
    
    protected abstract class ShardedRequestFuture<T> extends PromiseTask<T, ShardedResponseMessage<?>>
            implements OperationFuture<ShardedResponseMessage<?>>, ShardedOperation {
        
        protected final AtomicReference<State> state;

        public ShardedRequestFuture(
                State state,
                T task) {
            this(state, task, LoggingPromise.create(logger, SettableFuturePromise.<ShardedResponseMessage<?>>create()));
        }

        public ShardedRequestFuture(
                State state,
                T task,
                Promise<ShardedResponseMessage<?>> promise) {
            super(task, promise);
            this.state = new AtomicReference<State>(state);
        }
    
        @Override
        public State state() {
            return state.get();
        }

        @Override
        public State call() throws Exception {
            switch (state()) {
            case COMPLETE:
            {
                assert (isDone());
                ShardedResponseMessage<?> result = Futures.get(this, ExecutionException.class);
                if (this.state.compareAndSet(State.COMPLETE, State.PUBLISHED)) {
                    callback.onSuccess(result);
                }
                break;
            }
            default:
                break;
            }
            
            return state();
        }
    }

    protected class NotificationTask
        extends ShardedRequestFuture<Message.ServerResponse<IWatcherEvent>> {

        protected final Identifier identifier;
        
        public NotificationTask(
                Message.ServerResponse<IWatcherEvent> task) {
            super(State.SUBMITTING, task);
            this.identifier = lookup.apply(ZNodeLabel.Path.of(task.record().getPath()));
        }

        @Override
        public Identifier getIdentifier() {
            return identifier;
        }

        @Override
        public int xid() {
            return task.xid();
        }

        @Override
        public State call() throws Exception {
            switch (state()) {
            case SUBMITTING:
            {
                Message.ServerResponse<?> unsharded = unshard(getIdentifier(), task());
                if (unsharded != null) {
                    if (state.compareAndSet(State.SUBMITTING, State.COMPLETE)) {
                        set(ShardedResponseMessage.of(getIdentifier(), unsharded));
                    }
                }
                break;
            }
            default:
                return super.call();
            }
            
            return state();
        }
    }

    protected class ShardedRequestTask extends ShardedRequestFuture<ShardedRequestMessage<?>> implements 
            ShardedOperation {

        protected final Promise<Message.ServerResponse<?>> response;
        
        public ShardedRequestTask(
                ShardedRequestMessage<?> task) {
            super(State.WAITING, task);
            this.response = SettableFuturePromise.create();
        }
        
        public Promise<Message.ServerResponse<?>> response() {
            return response;
        }

        @Override
        public Identifier getIdentifier() {
            return task.getIdentifier();
        }

        @Override
        public int xid() {
            return task.xid();
        }

        @Override
        public State call() throws Exception {
            switch (state()) {
            case WAITING:
            {
                Message.ClientRequest<?> sharded = shard(getIdentifier(), task.getRequest());
                if (sharded != null) {
                    if (this.state.compareAndSet(State.WAITING, State.SUBMITTING)) {  
                        try {
                            ListenableFuture<? extends Message.ClientRequest<?>> writeFuture = connection.write(sharded);
                            Futures.addCallback(writeFuture, ShardedClientExecutor.this, sameThreadExecutor);
                        } catch (Throwable t) {
                            onFailure(t);
                        }
                    }
                }
                break;
            }
            case SUBMITTING:
            {
                if (response.isDone()) {
                    Message.ServerResponse<?> unsharded = unshard(getIdentifier(), response.get());
                    if (unsharded != null) {
                        if (this.state.compareAndSet(State.SUBMITTING, State.COMPLETE)) {  
                            set(ShardedResponseMessage.of(getIdentifier(), unsharded));
                        }
                    }
                }
                break;
            }
            default:
                return super.call();
            }
            return state();
        }
    }
}
