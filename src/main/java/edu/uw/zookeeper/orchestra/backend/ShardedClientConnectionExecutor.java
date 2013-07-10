package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionResponseMessage;
import edu.uw.zookeeper.protocol.SessionRequestMessage;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class ShardedClientConnectionExecutor<C extends Connection<? super Message.ClientSession>> extends ClientConnectionExecutor<C> {

    public static <C extends Connection<? super Message.ClientSession>> ShardedClientConnectionExecutor<C> newInstance(
            Publisher publisher,
            ShardedOperationTranslators translator,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ConnectMessage.Request request,
            C connection) {
        return new ShardedClientConnectionExecutor<C>(
                publisher,
                lookup,
                translator,
                request, 
                connection,
                AssignXidProcessor.newInstance(),
                connection);
    }

    protected final Publisher publisher;
    protected final Function<ZNodeLabel.Path, Identifier> lookup;
    protected final ShardedOperationTranslators translator;
    
    public ShardedClientConnectionExecutor(
            Publisher publisher,
            Function<ZNodeLabel.Path, Identifier> lookup,
            ShardedOperationTranslators translator,
            ConnectMessage.Request request,
            C connection,
            AssignXidProcessor xids,
            Executor executor) {
        super(request, connection, xids, executor);
        this.publisher = publisher;
        this.lookup = lookup;
        this.translator = translator;
    }
    
    @Override
    public ListenableFuture<Pair<Operation.SessionRequest, Operation.SessionResponse>> submit(Operation.Request request) {
        return submit(request, SettableFuturePromise.<Pair<Operation.SessionRequest, Operation.SessionResponse>>create());
    }

    @Override
    public ListenableFuture<Pair<Operation.SessionRequest, Operation.SessionResponse>> submit(Operation.Request request, Promise<Pair<Operation.SessionRequest, Operation.SessionResponse>> promise) {
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
                            : ((Operation.SessionRequest) request).request();

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
                    translated = SessionRequestMessage.newInstance(
                            ((Operation.SessionRequest) request).xid(), record);
                }
            } 
            if (translated instanceof Message.ClientRequest) {
                sharded = ShardedRequestMessage.of(id, (Message.ClientRequest) translated);
            } else {
                sharded = ShardedRequest.of(id, translated);
            }
        }
        return sharded;
    }

    @Override
    public void register(Object handler) {
        publisher.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        publisher.unregister(handler);
    }

    @Override
    public void post(Object event) {
        publisher.post(event);
    }

    @Override
    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        super.handleTransition(event);
        
        post(event);
    }

    @Override
    @Subscribe
    public void handleResponse(Message.ServerResponse message) throws InterruptedException {
        if (state.get() != State.TERMINATED) {
            received.put(message);
            schedule();
        }
    }

    @Override
    protected void applyReceived(PendingTask task, Message.ServerResponse response) {
        ShardedResponseMessage unshardedResponse;

        if ((task != null) && (task.task().xid() == response.xid())) {
            ShardedRequestTask shardedTask = (ShardedRequestTask) task.delegate();
            Identifier id = shardedTask.sharded().getId();
            Operation.Request input = shardedTask.input();
            ShardedRequestMessage unshardedRequest;
            if (input instanceof ShardedRequestMessage) {
                unshardedRequest = (ShardedRequestMessage) input;
            } else {
                Message.ClientRequest request;
                if (input instanceof Message.ClientRequest) {
                    request = (Message.ClientRequest) input;
                } else {
                    request = SessionRequestMessage.newInstance(
                                task.task().xid(),
                                (Records.Request) input);
                }
                unshardedRequest = ShardedRequestMessage.of(id, request);
            }

            Records.Response record = response.response();
            Records.Response translated = translator.get(id).apply(record);
            if (translated == record) {
                unshardedResponse = ShardedResponseMessage.of(id, response);
            } else {
                unshardedResponse = ShardedResponseMessage.of(
                        id,
                        SessionResponseMessage.newInstance(
                                response.xid(), response.zxid(), translated));
            }
            
            Pair<Operation.SessionRequest, Operation.SessionResponse> result = 
                    Pair.<Operation.SessionRequest, Operation.SessionResponse>create(unshardedRequest, unshardedResponse);
            task.set(result);
        } else {
            Identifier id = null;
            Records.Response record = response.response();
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
                            SessionResponseMessage.newInstance(
                                    response.xid(), response.zxid(), translated));
                }
            } else {
                unshardedResponse = ShardedResponseMessage.of(id, response);
            }
        }

        post(unshardedResponse);
    }

    protected static class ShardedRequestTask extends PromiseTask<Operation.Request, Pair<Operation.SessionRequest, Operation.SessionResponse>> {

        protected final Operation.Request input;
        protected final ShardedRequest<?> sharded;
        
        public ShardedRequestTask(
                Operation.Request input,
                Promise<Pair<Operation.SessionRequest, Operation.SessionResponse>> delegate,
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
