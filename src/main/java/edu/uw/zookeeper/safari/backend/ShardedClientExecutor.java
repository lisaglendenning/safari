package edu.uw.zookeeper.safari.backend;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.client.PendingQueueClientExecutor;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class ShardedClientExecutor<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends PendingQueueClientExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>, ShardedClientExecutor.ShardedRequestTask, C> {

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
    // not thread-safe
    protected final Set<ListenableFuture<OperationPrefixTranslator>> futures;
    
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
        this.futures = Collections.newSetFromMap(
                new WeakHashMap<ListenableFuture<OperationPrefixTranslator>, Boolean>());
    }

    @Override
    public ListenableFuture<ShardedResponseMessage<?>> submit(
            ShardedRequestMessage<?> request, Promise<ShardedResponseMessage<?>> promise) {
        ListenableFuture<OperationPrefixTranslator> future;
        try {
            future = translator.apply(request.getIdentifier());
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
        ShardedRequestTask task = new ShardedRequestTask(
                future,
                request, 
                LoggingPromise.create(logger, promise));
        Futures.addCallback(task, callback, sameThreadExecutor);
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    @Subscribe
    public void handleResponse(Operation.ProtocolResponse<?> response) {
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
        
        super.handleResponse(unsharded);

        if (response.xid() == OpCodeXid.NOTIFICATION.xid()) {
            callback.onSuccess(unsharded);
        }
    }
    
    @Override
    protected void doRun() throws Exception {
        ShardedRequestTask next;
        while ((next = mailbox.peek()) != null) {
            if (next.translator().isDone()) {
                mailbox.remove(next);
                if (!apply(next)) {
                    break;
                }
            } else {
                if (futures.add(next.translator())) {
                    next.translator().addListener(this, sameThreadExecutor);
                }
                break;
            }
        }
    }
    
    @Override
    protected boolean apply(ShardedRequestTask input) throws Exception {
        assert (input.translator().isDone());
        if (! input.isDone()) {
            if (state() != State.TERMINATED) {
                write(input.shard(), input.promise());
            } else {
                input.cancel(true);
            }
        }
        
        return (state() != State.TERMINATED);
    }
    
    protected static class ShardedRequestTask extends PendingQueueClientExecutor.RequestTask<ShardedRequestMessage<?>, ShardedResponseMessage<?>> {

        protected final ListenableFuture<OperationPrefixTranslator> translator;
        
        public ShardedRequestTask(
                ListenableFuture<OperationPrefixTranslator> translator,
                ShardedRequestMessage<?> task,
                Promise<ShardedResponseMessage<?>> promise) {
            super(task, promise);
            this.translator = translator;
        }
        
        public ListenableFuture<OperationPrefixTranslator> translator() {
            return translator;
        }
        
        public Message.ClientRequest<?> shard() throws InterruptedException, ExecutionException {
            return translator.get().apply(task.getRequest());
        }
    }
}
