package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.client.AssignXidProcessor;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;

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
    public ListenableFuture<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> submit(Operation.Request request) {
        return submit(request, SettableFuturePromise.<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>>create());
    }

    @Override
    public ListenableFuture<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> submit(Operation.Request request, Promise<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> promise) {
        ShardedRequestTask task = new ShardedRequestTask(request, promise, shard(request));
        send(task);
        return task;
    }
    
    public ShardedRequest<?> shard(Operation.Request request) {
        ShardedRequest<?> sharded;
        if (request instanceof ShardedRequest) {
            sharded = (ShardedRequest<?>) request;
        } else {
            Records.Request record = (request instanceof Records.Request)
                    ? (Records.Request) request 
                            : ((Operation.ProtocolRequest<?>) request).getRecord();

            Identifier id = null;
            if (record instanceof IMultiRequest) {
                // TODO
                throw new UnsupportedOperationException();
            } else if (record instanceof Records.PathGetter) {
                id = lookup.apply(ZNodeLabel.Path.of(((Records.PathGetter) record).getPath()));
            } 
            Operation.Request translated = request;
            if (id != null) {
                record = translator.get(id).apply(record);
                if (request instanceof Records.Request) {
                    translated = record;
                } else {
                    translated = ProtocolRequestMessage.of(
                            ((Operation.ProtocolRequest<?>) request).getXid(), record);
                }
            } 
            if (translated instanceof Message.ClientRequest) {
                sharded = ShardedRequestMessage.of(id, (Message.ClientRequest<?>) translated);
            } else {
                sharded = ShardedRequest.of(id, translated);
            }
        }
        return sharded;
    }

    @Override
    @Subscribe
    public void handleResponse(Message.ServerResponse<?> message) throws InterruptedException {
        if ((state.get() != State.TERMINATED) && ! (message instanceof ShardedResponseMessage)) {
            receive(message);
        }
    }

    @Override
    protected void applyReceived(PendingTask task, Message.ServerResponse<?> response) {
        ShardedResponseMessage<?> unshardedResponse;

        if ((task != null) && (task.task().getXid() == response.getXid())) {
            ShardedRequestTask shardedTask = (ShardedRequestTask) task.delegate();
            Identifier id = shardedTask.sharded().getId();
            Operation.Request input = shardedTask.input();
            ShardedRequestMessage<?> unshardedRequest;
            if (input instanceof ShardedRequestMessage) {
                unshardedRequest = (ShardedRequestMessage<?>) input;
            } else {
                Message.ClientRequest<?> request;
                if (input instanceof Message.ClientRequest) {
                    request = (Message.ClientRequest<?>) input;
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
            
            Pair<Message.ClientRequest<?>, Message.ServerResponse<?>> result = 
                    Pair.<Message.ClientRequest<?>, Message.ServerResponse<?>>create(unshardedRequest, unshardedResponse);
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

    protected static class ShardedRequestTask extends PromiseTask<Operation.Request, Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> {

        protected final Operation.Request input;
        protected final ShardedRequest<?> sharded;
        
        public ShardedRequestTask(
                Operation.Request input,
                Promise<Pair<Message.ClientRequest<?>, Message.ServerResponse<?>>> delegate,
                ShardedRequest<?> sharded) {
            super(sharded.getRequest(), delegate);
            this.input = input;
            this.sharded = sharded;
        }
        
        public Operation.Request input() {
            return input;
        }
        
        public ShardedRequest<?> sharded() {
            return sharded;
        }
    }
}
