package edu.uw.zookeeper.safari.backend;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.client.PendingQueueClientExecutor;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class ShardedClientExecutor<C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> extends PendingQueueClientExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>, ShardedClientExecutor.ShardedRequestTask, C> {

    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> ShardedClientExecutor<C> newInstance(
            Function<ZNodeLabel.Path, Identifier> lookup,
            CachedFunction<Identifier, OperationPrefixTranslator> translator,
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                lookup,
                translator,
                ConnectTask.connect(connection, request),
                connection,
                TimeValue.milliseconds(request.getTimeOut()),
                executor,
                new StrongConcurrentSet<SessionListener>());
    }

    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> ShardedClientExecutor<C> newInstance(
            Function<ZNodeLabel.Path, Identifier> lookup,
            CachedFunction<Identifier, OperationPrefixTranslator> translator,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            IConcurrentSet<SessionListener> listeners) {
        return new ShardedClientExecutor<C>(
                lookup,
                translator,
                session, 
                connection,
                timeOut,
                executor,
                listeners,
                connection,
                LogManager.getLogger(ShardedClientExecutor.class));
    }

    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final CachedFunction<Identifier, OperationPrefixTranslator> translator;
    // not thread-safe
    protected final Set<ListenableFuture<?>> futures;
    
    protected ShardedClientExecutor(
            Function<ZNodeLabel.Path, Identifier> lookup,
            CachedFunction<Identifier, OperationPrefixTranslator> translator,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler,
            IConcurrentSet<SessionListener> listeners,
            Executor executor,
            Logger logger) {
        super(session, connection, timeOut, scheduler, listeners, executor, logger);
        this.lookup = lookup;
        this.translator = translator;
        this.futures = Collections.newSetFromMap(
                new WeakHashMap<ListenableFuture<?>, Boolean>());
    }

    @Override
    public ListenableFuture<ShardedResponseMessage<?>> submit(
            ShardedRequestMessage<?> request, Promise<ShardedResponseMessage<?>> promise) {
        ShardedRequestTask task = new ShardedRequestTask(
                request, 
                LoggingPromise.create(logger, promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    public void handleConnectionRead(Operation.Response response) {
        if (response instanceof Message.ServerResponse) {
            ShardedResponseMessage<?> unsharded;
            Message.ServerResponse<?> message = (Message.ServerResponse<?>) response;
            if (message.record() instanceof Records.PathGetter) {
                Identifier shard = lookup.apply(ZNodeLabel.Path.of(((Records.PathGetter) message.record()).getPath()));
                assert (shard != null);
                OperationPrefixTranslator translator = this.translator.cached().apply(shard);
                assert (translator != null);
                unsharded = ShardedResponseMessage.of(shard, translator.apply(message));
            } else {
                unsharded = ShardedResponseMessage.of(Identifier.zero(), message);
            }
            response = unsharded;
        }
        synchronized (pending) {
            super.handleConnectionRead(response);
            runPending();
        }
    }

    @Override
    protected synchronized boolean schedule() {
        Iterator<ListenableFuture<?>> itr = futures.iterator();
        while (itr.hasNext()) {
            ListenableFuture<?> next = itr.next();
            if (next.isDone()) {
                itr.remove();
            }
        }
        if (futures.isEmpty()) {
            return super.schedule();
        } else {
            return false;
        }
    }

    @Override
    protected void doRun() throws Exception {
        ShardedRequestTask next;
        while ((next = mailbox.peek()) != null) {
            logger.debug("Applying {} ({})", next, this);
            if (!apply(next)) {
                break;
            }
        }
    }
    
    @Override
    protected boolean apply(ShardedRequestTask input) {
        if (! input.isDone()) {
            ListenableFuture<OperationPrefixTranslator> lookup;
            try {
                lookup = translator.apply(input.task().getIdentifier());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            if (! lookup.isDone()) {
                if (futures.add(lookup)) {
                    lookup.addListener(this, SAME_THREAD_EXECUTOR);
                }
                return false;
            }
            if (mailbox.remove(input)) {
                Message.ClientRequest<?> sharded = null;
                try {
                    sharded = input.apply(lookup.get());
                } catch (ExecutionException e) {
                    // we add failed requests to the pending queue
                    // to ensure that the future is set with respect
                    // to request ordering
                    FailedRequestTask failed = new FailedRequestTask(
                            new ShardedRequestException(input.task(), e.getCause()),
                            input.task().xid(),
                            this,
                            input.promise());
                    pending.add(failed);
                    runPending();
                    sharded = null;
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                if (sharded != null) {
                    write(sharded, input.promise());
                }
            }
        } else {
            logger.warn("{}", input);
            mailbox.remove(input);
        }
        
        return (state() != State.TERMINATED);
    }

    protected void runPending() {
        synchronized (pending) {
            Iterator<PendingTask<ShardedResponseMessage<?>>> itr = pending.iterator();
            while (itr.hasNext()) {
                PendingTask<ShardedResponseMessage<?>> next = itr.next();
                if (next instanceof Runnable) {
                    itr.remove();
                    ((Runnable) next).run();
                } else {
                    break;
                }
            }
        }
    }
    
    public static class ShardedRequestException extends Exception {
    
        private static final long serialVersionUID = -6523271323477641540L;
        
        private final ShardedRequestMessage<?> request;
        
        public ShardedRequestException(ShardedRequestMessage<?> request, Throwable cause) {
            super(String.format("%s failed", request), cause);
            this.request = request;
        }
        
        public ShardedRequestMessage<?> request() {
            return request;
        }
    }

    protected static class ShardedRequestTask extends PendingQueueClientExecutor.RequestTask<ShardedRequestMessage<?>, ShardedResponseMessage<?>> implements Function<OperationPrefixTranslator, Message.ClientRequest<?>> {

        public ShardedRequestTask(
                ShardedRequestMessage<?> task,
                Promise<ShardedResponseMessage<?>> promise) {
            super(task, promise);
            assert(task.xid() != OpCodeXid.PING.xid());
        }
        
        @Override
        public Message.ClientRequest<?> apply(OperationPrefixTranslator input) {
            return input.apply(task.getRequest());
        }
    }
    
    protected static class FailedRequestTask extends PendingQueueClientExecutor.PendingTask<ShardedResponseMessage<?>> implements Runnable {

        protected final Exception failure;
        
        public FailedRequestTask(
                Exception failure,
                int xid,
                FutureCallback<? super PendingTask<ShardedResponseMessage<?>>> callback,
                Promise<ShardedResponseMessage<?>> promise) {
            super(xid, callback, promise);
            this.failure = failure;
        }

        @Override
        public void run() {
            setException(failure);
        }

        @Override
        protected Objects.ToStringHelper toString(Objects.ToStringHelper toString) {
            return super.toString(toString).add("failure", failure);
        }
    }
}
