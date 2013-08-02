package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedOperation;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.client.AssignXidProcessor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public class ShardedClientConnectionExecutor<C extends Connection<? super Message.ClientSession>> extends ClientConnectionExecutor<C> {

    public static <C extends Connection<? super Message.ClientSession>> ShardedClientConnectionExecutor<C> newInstance(
            ShardedOperationTranslators translator,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ListenableFuture<ConnectMessage.Response> session,
            C connection) {
        return new ShardedClientConnectionExecutor<C>(
                lookup,
                translator,
                session, 
                connection,
                AssignXidProcessor.newInstance(),
                connection);
    }

    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final ShardedOperationTranslators translator;
    
    public ShardedClientConnectionExecutor(
            Function<ZNodeLabel.Path, Identifier> lookup,
            ShardedOperationTranslators translator,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            AssignXidProcessor xids,
            Executor executor) {
        super(session, connection, xids, executor);
        this.lookup = lookup;
        this.translator = translator;
    }
    
    @Override
    public ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> submit(Operation.Request request) {
        return submit(request, SettableFuturePromise.<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>>create());
    }

    @Override
    public ListenableFuture<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> submit(Operation.Request request, Promise<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> promise) {
        ShardedRequestTask task = new ShardedRequestTask(request, promise, shard(request));
        send(task);
        return task;
    }
    
    @SuppressWarnings("unchecked")
    public ShardedOperation.Request<?> shard(Operation.Request request) {
        Records.Request record = (request instanceof Records.Request) ?
                (Records.Request) request :
                ((Operation.RecordHolder<Records.Request>) request).getRecord();
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
    public void handleResponse(Message.ServerResponse<Records.Response> message) throws InterruptedException {
        if ((state.get() != State.TERMINATED) && ! (message instanceof ShardedResponseMessage)) {
            receive(message);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void applyReceived(PendingTask task, Message.ServerResponse<Records.Response> response) {
        ShardedResponseMessage<Records.Response> unshardedResponse;

        if ((task != null) && (task.task().getXid() == response.getXid())) {
            ShardedRequestTask shardedTask = (ShardedRequestTask) task.delegate();
            Identifier id = shardedTask.sharded().getIdentifier();
            Operation.Request input = shardedTask.input();
            ShardedRequestMessage<Records.Request> unshardedRequest;
            if (input instanceof ShardedRequestMessage) {
                unshardedRequest = (ShardedRequestMessage<Records.Request>) input;
            } else {
                Message.ClientRequest<Records.Request> request;
                if (input instanceof Message.ClientRequest) {
                    request = (Message.ClientRequest<Records.Request>) input;
                } else {
                    request = ProtocolRequestMessage.of(
                                task.task().getXid(),
                                (Records.Request) input);
                }
                unshardedRequest = ShardedRequestMessage.of(id, request);
            }

            Records.Response record = response.getRecord();
            Records.Response translated = translator.get(id).apply(record);
            if (translated == record) {
                unshardedResponse = ShardedResponseMessage.of(id, response);
            } else {
                unshardedResponse = ShardedResponseMessage.of(
                        id,
                        ProtocolResponseMessage.of(
                                response.getXid(), response.getZxid(), translated));
            }
            
            Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> result = 
                    Pair.<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>create(unshardedRequest, unshardedResponse);
            task.set(result);
        } else {
            Identifier id = null;
            Records.Response record = response.getRecord();
            if (record instanceof Records.PathGetter) {
                id = lookup.apply(ZNodeLabel.Path.of(((Records.PathGetter) record).getPath()));
            }
            if (id != null) {
                Records.Response translated = translator.get(id).apply(record);
                if (translated == record) {
                    unshardedResponse = ShardedResponseMessage.of(id, response);
                } else {
                    unshardedResponse = ShardedResponseMessage.of(
                            id,
                            ProtocolResponseMessage.of(
                                    response.getXid(), response.getZxid(), translated));
                }
            } else {
                unshardedResponse = ShardedResponseMessage.of(id, response);
            }
        }

        post(unshardedResponse);
    }

    protected static class ShardedRequestTask extends PromiseTask<Operation.Request, Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> {

        protected final Operation.Request input;
        protected final ShardedOperation.Request<?> sharded;
        
        public ShardedRequestTask(
                Operation.Request input,
                Promise<Pair<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>>> delegate,
                ShardedOperation.Request<?> sharded) {
            super(sharded.getRequest(), delegate);
            this.input = input;
            this.sharded = sharded;
        }
        
        public Operation.Request input() {
            return input;
        }
        
        public ShardedOperation.Request<?> sharded() {
            return sharded;
        }
    }
}
