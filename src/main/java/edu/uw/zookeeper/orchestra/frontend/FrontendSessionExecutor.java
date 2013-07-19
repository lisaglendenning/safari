package edu.uw.zookeeper.orchestra.frontend;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.backend.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.backend.ShardedResponseMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Request;
import edu.uw.zookeeper.protocol.proto.Records.Response;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TaskExecutor;

public class FrontendSessionExecutor extends AbstractActor<FrontendSessionExecutor.RequestTask> implements TaskExecutor<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> {

    protected final long sessionId;
    protected final Publisher publisher;
    protected final CachedFunction<ZNodeLabel.Path, Volume> volumeLookup;
    protected final TaskExecutor<MessageSessionRequest, MessageSessionRequest> requestExecutor;

    protected FrontendSessionExecutor(
            long sessionId,
            Publisher publisher,
            CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
            TaskExecutor<MessageSessionRequest, MessageSessionRequest> volumeExecutor,
            Executor executor) {
        super(executor, FutureQueue.<RequestTask>create(), AbstractActor.newState());
        this.sessionId = sessionId;
        this.publisher = publisher;
        this.volumeLookup = volumeLookup;
        this.requestExecutor = volumeExecutor;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
            Message.ClientRequest<Records.Request> request) {
        Promise<Message.ServerResponse<Response>> promise = SettableFuturePromise.create();
        RequestTask task = new RequestTask(request, promise);
        send(task);
        return task;
    }
    
    @Subscribe
    public void handleResponse(ShardedResponseMessage<Records.Response> message) {
        for (RequestTask request: mailbox) {
            if (request.task().getXid() == message.getXid()) {
                ResponseTask response = request.submitted().get(message.getId());
                if (response == null) {
                    // FIXME
                    throw new AssertionError();
                }
                if (! response.set(message)) {
                    // FIXME
                    throw new AssertionError();
                }
            }
        }
    }

    @Override
    public void send(RequestTask message) {
        super.send(message);
        message.addListener(this, MoreExecutors.sameThreadExecutor());
    }
    
    @Override
    protected boolean runEnter() {
        if (state.get() == State.WAITING) {
            schedule();
            return false;
        } else {
            return super.runEnter();
        }
    }

    @Override
    protected void doRun() throws Exception {
        doRequests();
        
        super.doRun();
    }
    
    protected void doRequests() throws Exception {
        RequestTask prev = null;
        for (RequestTask task: mailbox) {
            switch (task.state()) {
            case NEW:
            case LOOKING:
            {
                ListenableFuture<List<Volume>> lookupFuture = task.lookup(volumeLookup);
                if (! lookupFuture.isDone()) {
                    lookupFuture.addListener(this, MoreExecutors.sameThreadExecutor());
                    break;
                }
                if ((prev != null) && (prev.state() == RequestState.LOOKING)) {
                    // we can't submit if a task before us hasn't submitted yet
                    break;
                }
                Set<Volume> volumes = Sets.newHashSet();
                for (Volume v: lookupFuture.get()) {
                    if (v == null) {
                        // TODO
                    } else {
                        volumes.add(v);
                    }
                }
                List<ShardedRequestMessage<Records.Request>> requests = task.shard(volumes);
                task.submit(sessionId, requests, requestExecutor);
                Futures.successfulAsList(task.submitted().values()).addListener(this, MoreExecutors.sameThreadExecutor());
            }
            case SUBMITTING:
            {
                // FIXME
            }
            default:
                break;
            }
            
            prev = task;
        }
    }

    @Override
    protected boolean apply(RequestTask input) throws Exception {
        boolean running = super.apply(input);
        publisher.post(input.get());
        return running;
    }
    
    protected static enum RequestState {
        NEW, LOOKING, SUBMITTING, COMPLETE;
    }
    
    protected static class RequestTask extends PromiseTask<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> implements Stateful<RequestState> {

        protected final AtomicReference<RequestState> state;
        // TODO: or immutablelist?
        protected final ZNodeLabel.Path[] paths;
        // TODO: atomicreferencearray ?
        protected final ListenableFuture<Volume>[] lookups;
        protected AtomicReference<ListenableFuture<List<Volume>>> lookupFuture;
        protected final ConcurrentMap<Identifier, ResponseTask> submitted;
        protected volatile ListenableFuture<List<ShardedResponseMessage<Records.Response>>> submittedFuture;
        
        @SuppressWarnings("unchecked")
        public RequestTask(
                Message.ClientRequest<Request> task,
                Promise<Message.ServerResponse<Response>> delegate) {
            super(task, delegate);
            this.state = new AtomicReference<RequestState>(RequestState.NEW);
            this.paths = PathsOfRequest.getPathsOfRequest(task.getRecord());
            if (paths.length > 1) {
                Arrays.sort(paths);
            }
            this.lookups = new ListenableFuture[paths.length];
            this.lookupFuture = new AtomicReference<ListenableFuture<List<Volume>>>((paths.length == 0) ?
                Futures.immediateFuture((List<Volume>) ImmutableList.<Volume>of()) : null);
            this.submitted = new MapMaker().makeMap();
            this.submittedFuture = null;
        }
        
        @Override
        public RequestState state() {
            return state.get();
        }

        public ZNodeLabel.Path[] paths() {
            return paths;
        }
        
        public ListenableFuture<Volume>[] lookups() {
            return lookups;
        }
        
        public ConcurrentMap<Identifier, ResponseTask> submitted() {
            return submitted;
        }
        
        public ListenableFuture<List<Volume>> lookup(AsyncFunction<ZNodeLabel.Path, Volume> lookup) throws Exception {
            this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
            // TODO: Arrays.fill(null) if there was a failure to start over...
            boolean changed = false;
            if (paths.length > 0) {
                for (int i=0; i< paths.length; ++i) {
                    if (lookups[i] == null) {
                        changed = true;
                        lookups[i] = lookup.apply(paths[i]);
                    }
                }
            }
            ListenableFuture<List<Volume>> prev = lookupFuture.get();
            if (changed || prev == null) {
                lookupFuture.compareAndSet(prev, Futures.successfulAsList(lookups));
            }
            return this.lookupFuture.get();
        }
        
        public List<ShardedRequestMessage<Records.Request>> shard(Set<Volume> volumes) {
            List<ShardedRequestMessage<Records.Request>> requests = Lists.newArrayListWithExpectedSize(Math.max(1, volumes.size()));
            if (OpCode.MULTI == task().getRecord().getOpcode()) {
                Map<Volume, IMultiRequest> multis = Maps.newHashMapWithExpectedSize(volumes.size());
                for (Volume volume: volumes) {
                    multis.put(volume, new IMultiRequest());
                }
                for (Records.MultiOpRequest op: (IMultiRequest) task().getRecord()) {
                    ZNodeLabel.Path[] paths = PathsOfRequest.getPathsOfRequest(op);
                    assert (paths.length > 0);
                    for (ZNodeLabel.Path path: paths) {
                        for (Map.Entry<Volume, IMultiRequest> multi: multis.entrySet()) {
                            if (multi.getKey().getDescriptor().contains(path)) {
                                multi.getValue().add(op);
                            }
                        }
                        // TODO: assert that a request went to at least one volume...
                    }
                }
                for (Map.Entry<Volume, IMultiRequest> multi: multis.entrySet()) {
                    requests.add(ShardedRequestMessage.of(
                                multi.getKey().getId(), 
                                ProtocolRequestMessage.of(
                                        task().getXid(), 
                                        (Records.Request) multi.getValue())));                            
                }
            } else {
                for (Volume volume: volumes) {
                    ShardedRequestMessage<Records.Request> request = ShardedRequestMessage.of(volume.getId(), task());
                    requests.add(request);                            
                }
            }
            return requests;
        }
        
        public void submit(long sessionId, List<ShardedRequestMessage<Records.Request>> requests, TaskExecutor<MessageSessionRequest, MessageSessionRequest> requestExecutor) {
            this.state.compareAndSet(RequestState.LOOKING, RequestState.SUBMITTING);
            
            for (ShardedRequestMessage<Records.Request> request: requests) {
                ListenableFuture<MessageSessionRequest> future = requestExecutor.submit(MessageSessionRequest.of(sessionId, request));
                ResponseTask task = new ResponseTask(request, future);
                submitted.put(request.getId(), task);
            }
        }
        
        public ShardedResponseMessage<Records.Response> complete() {
            for (ResponseTask response: submitted.values()) {
                if (! response.isDone()) {
                    return null;
                }
            }
            // FIXME
            return null;
        }
    }

    protected static class ResponseTask extends PromiseTask<ShardedRequestMessage<Records.Request>, ShardedResponseMessage<Records.Response>> implements Runnable {

        protected final ListenableFuture<MessageSessionRequest> future;

        public ResponseTask(
                ShardedRequestMessage<Records.Request> request,
                ListenableFuture<MessageSessionRequest> future) {
            this(request, future, SettableFuturePromise.<ShardedResponseMessage<Response>>create());
        }
        
        public ResponseTask(
                ShardedRequestMessage<Records.Request> request,
                ListenableFuture<MessageSessionRequest> future,
                Promise<ShardedResponseMessage<Response>> delegate) {
            super(request, delegate);
            this.future = future;
            future.addListener(this, MoreExecutors.sameThreadExecutor());
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                future.cancel(mayInterruptIfRunning);
            }
            return cancelled;
        }
        
        @Override
        public void run() {
            if (future.isDone()) {
                try {
                    future.get();
                } catch (Throwable t) {
                    setException(t);
                }
            }
        }
    }
}
