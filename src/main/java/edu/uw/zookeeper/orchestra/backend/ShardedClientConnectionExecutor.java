package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedOperation;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.client.AssignXidProcessor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;

public class ShardedClientConnectionExecutor<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> extends ClientConnectionExecutor<C> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ShardedClientConnectionExecutor<C> newInstance(
            ShardedOperationTranslators translator,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                translator,
                lookup,
                request,
                AssignXidProcessor.newInstance(),
                connection,
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ShardedClientConnectionExecutor<C> newInstance(
            ShardedOperationTranslators translator,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ConnectMessage.Request request,
            AssignXidProcessor xids,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                translator,
                lookup,
                ConnectTask.create(connection, request),
                xids,
                connection,
                TimeValue.create(Long.valueOf(request.getTimeOut()), TimeUnit.MILLISECONDS),
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ShardedClientConnectionExecutor<C> newInstance(
            ShardedOperationTranslators translator,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ListenableFuture<ConnectMessage.Response> session,
            AssignXidProcessor xids,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new ShardedClientConnectionExecutor<C>(
                lookup,
                translator,
                session, 
                xids,
                connection,
                timeOut,
                executor);
    }

    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final ShardedOperationTranslators translator;
    
    public ShardedClientConnectionExecutor(
            Function<ZNodeLabel.Path, Identifier> lookup,
            ShardedOperationTranslators translator,
            ListenableFuture<ConnectMessage.Response> session,
            AssignXidProcessor xids,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(session, xids, connection, timeOut, executor);
        this.lookup = lookup;
        this.translator = translator;
    }

    public ShardedOperation.Request<?> shard(Operation.Request request) {
        Records.Request record = 
                (Records.Request) ((request instanceof Records.Request) ?
                 request :
                ((Operation.RecordHolder<?>) request).getRecord());
        Identifier shard = Identifier.zero();
        if (request instanceof ShardedOperation) {
            shard = ((ShardedOperation) request).getIdentifier();
        } else {
            if (record instanceof IMultiRequest) {
                throw new UnsupportedOperationException();
            } else if (record instanceof Records.PathGetter) {
                shard = lookup.apply(ZNodeLabel.Path.of(((Records.PathGetter) record).getPath()));
            } 
        }
        if (shard == null) {
            // TODO
            throw new UnsupportedOperationException();
        }
        record = translator.get(shard).apply(record);
        ShardedOperation.Request<?> sharded;
        if (request instanceof Records.Request) {
            sharded = ShardedRequest.of(
                    shard, record);
        } else {
            sharded = ShardedRequestMessage.of(
                    shard,
                    ProtocolRequestMessage.of(
                        ((Operation.ProtocolRequest<?>) request).getXid(), record));
        }
        return sharded;
    }

    @Override
    @Subscribe
    public void handleResponse(Message.ServerResponse<?> message) {
        if ((state() != State.TERMINATED) && !(message instanceof ShardedOperation)) {
            timeOut.send(message);
            int xid = message.getXid();
            if (xid != OpCodeXid.PING.getXid()) {
                ShardedResponseMessage<?> unshardedResponse;
                PendingResponseTask next = pending.peek();
                if ((next != null) && (next.getXid() == xid)) {
                    pending.remove(next);
                    Identifier id = ((ShardedRequestTask) next.delegate()).getIdentifier();
                    Records.Response record = message.getRecord();
                    Records.Response translated = translator.get(id).apply(record);
                    if (translated == record) {
                        unshardedResponse = ShardedResponseMessage.of(id, message);
                    } else {
                        unshardedResponse = ShardedResponseMessage.of(
                                id,
                                ProtocolResponseMessage.of(
                                        xid, message.getZxid(), translated));
                    }
                    
                    next.set(unshardedResponse);
                } else {
                    if (xid != OpCodeXid.NOTIFICATION.getXid()) {
                        // FIXME is this an error?
                        logger.warn("{} != {} ({})", next, message, this);
                    }
                    
                    Identifier id = Identifier.zero();
                    Records.Response record = message.getRecord();
                    if (record instanceof Records.PathGetter) {
                        id = lookup.apply(ZNodeLabel.Path.of(((Records.PathGetter) record).getPath()));
                    }
                    if (! Identifier.zero().equals(id)) {
                        Records.Response translated = translator.get(id).apply(record);
                        if (translated == record) {
                            unshardedResponse = ShardedResponseMessage.of(id, message);
                        } else {
                            unshardedResponse = ShardedResponseMessage.of(
                                    id,
                                    ProtocolResponseMessage.of(
                                            xid, message.getZxid(), translated));
                        }
                    } else {
                        unshardedResponse = ShardedResponseMessage.of(id, message);
                    }
                }
    
                post(unshardedResponse);
            }
        }
    }
    
    @Override
    protected boolean apply(PromiseTask<Operation.Request, Message.ServerResponse<?>> input) {
        return super.apply(new ShardedRequestTask(shard(input.task()), input));
    }
    
    protected static class ShardedRequestTask extends PromiseTask<Operation.Request, Message.ServerResponse<?>> implements ShardedOperation {

        protected final ShardedOperation.Request<?> sharded;
        
        public ShardedRequestTask(
                ShardedOperation.Request<?> sharded,
                Promise<Message.ServerResponse<?>> delegate) {
            super(sharded.getRequest(), delegate);
            this.sharded = sharded;
        }

        @Override
        public Identifier getIdentifier() {
            return sharded.getIdentifier();
        }
    }
}
