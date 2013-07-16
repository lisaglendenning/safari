package edu.uw.zookeeper.orchestra.frontend;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import io.netty.buffer.ByteBuf;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ForwardingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.VolumeAssignment;
import edu.uw.zookeeper.orchestra.VolumeLookupService;
import edu.uw.zookeeper.orchestra.backend.ShardedOperationTranslators;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.client.ClientProtocolCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.PublisherActor;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.TaskExecutor;

public class FrontendRequestExecutor extends AbstractActor<FrontendRequestExecutor.Task> implements Publisher, Executor, TaskExecutor<ByteBuf, ByteBuf> {

    public static enum PathsOfRequest implements Function<Operation.Request, List<ZNodeLabel.Path>> {
        INSTANCE;

        public static PathsOfRequest getInstance() {
            return INSTANCE;
        }
        
        @Override
        public List<ZNodeLabel.Path> apply(Operation.Request input) {
            ImmutableList.Builder<ZNodeLabel.Path> paths = ImmutableList.builder();
            if (input instanceof IMultiRequest) {
                for (Records.MultiOpRequest e: (IMultiRequest)input) {
                    paths.addAll(apply(e));
                }
            } else if (input instanceof Records.PathGetter) {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) input).getPath());
                paths.add(path);
            }
            return paths.build();
        }
    }
    
    
    public static FrontendRequestExecutor newInstance(
            Publisher publisher,
            Executor executor,
            long sessionId,
            ClientConnectionExecutor<?> client,
            VolumeLookupService lookup,
            ShardedOperationTranslators translator) {
        return new FrontendRequestExecutor(publisher, executor, sessionId, client, lookup, translator);
    }
    
    protected final long sessionId;
    protected final ServerProtocolCodec serverCodec;
    protected final ClientProtocolCodec clientCodec;
    protected final ClientConnectionExecutor<?> client;
    protected final VolumeLookupService lookup;
    protected final BackendActor backendActor;
    protected final PublisherActor publisher;
    protected final ShardedOperationTranslators translator;
    
    protected FrontendRequestExecutor(
            Publisher publisher,
            Executor executor,
            long sessionId,
            ClientConnectionExecutor<?> client,
            VolumeLookupService lookup,
            ShardedOperationTranslators translator) {
        super(executor, AbstractActor.<Task>newQueue(), AbstractActor.newState());
        this.publisher = PublisherActor.newInstance(publisher, this);
        this.sessionId = sessionId;
        this.client = client;
        this.serverCodec = ServerProtocolCodec.newInstance(this, ProtocolState.CONNECTED);
        this.clientCodec = ClientProtocolCodec.newInstance(this, ProtocolState.CONNECTED);
        this.lookup = lookup;
        this.backendActor = new BackendActor(this);
        this.translator = translator;
        
        client.register(this);
    }
    
    @Override
    public ListenableFuture<ByteBuf> submit(ByteBuf request) {
        return submit(request, SettableFuturePromise.<ByteBuf>create());
    }

    public ListenableFuture<ByteBuf> submit(
            ByteBuf request,
            Promise<ByteBuf> promise) {
        Task task = new Task(request, promise);
        send(task);
        return task;
    }

    @Override
    protected boolean apply(Task input) throws Exception {
        backendActor.send(new LookupTask(input));
        return true;
    }
    
    @Subscribe
    public void handleSessionReply(Operation.ProtocolResponse<?> message) {
        if (OpCodeXid.NOTIFICATION.getXid() == message.getXid()) {
            try {
                // TODO
            } catch (Exception e) {
                // TODO
                throw Throwables.propagate(e);
            }
        }
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }

    @Override
    public void post(Object object) {
        publisher.post(object);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }
    
    public static class Task extends PromiseTask<ByteBuf, ByteBuf> {
        public Task(ByteBuf task, Promise<ByteBuf> delegate) {
            super(task, delegate);
        }
    }
    
    protected class LookupTask extends ForwardingListenableFuture<List<VolumeAssignment>> {
        
        protected final Task task;
        protected final Operation.ProtocolRequest<?> message;
        protected final List<ZNodeLabel.Path> paths;
        protected final ListenableFuture<List<VolumeAssignment>> delegate;

        protected LookupTask(Task task) throws IOException {
            super();
            this.task = task;
            this.message = (Operation.ProtocolRequest<?>) serverCodec.decode(task.task()).get();
            this.paths = PathsOfRequest.getInstance().apply(message.getRecord());
            
            ListenableFuture<List<VolumeAssignment>> delegate = null;
            ImmutableList.Builder<ListenableFuture<VolumeAssignment>> futures = ImmutableList.builder();
            try {
                for (ZNodeLabel.Path path: paths) {
                    futures.add(lookup.lookup(path));
                }
                delegate = Futures.allAsList(futures.build());
            } catch (Exception e) {
                delegate = Futures.immediateFailedCheckedFuture(e);
            }
            this.delegate = delegate;
        }
        
        public Task getTask() {
            return task;
        }
        
        public Operation.ProtocolRequest<?> getMessage() {
            return message;
        }
        
        public List<ZNodeLabel.Path> getPaths() {
            return paths;
        }
        
        @Override
        protected ListenableFuture<List<VolumeAssignment>> delegate() {
            return delegate;
        }
    }
    
    protected class BackendActor extends AbstractActor<LookupTask> {

        protected BackendActor(Executor executor) {
            super(executor, new FutureQueue<List<VolumeAssignment>, LookupTask>(), AbstractActor.newState());
        }

        @Override
        public void send(LookupTask task) {
            super.send(task);
            task.addListener(this, executor);
        }
        
        @Override
        protected boolean runEnter() {
            if (State.WAITING == state.get()) {
                schedule();
                return false;
            } else {
                return super.runEnter();
            }
        }

        @Override
        protected boolean apply(LookupTask input) throws Exception {
            // TODO Auto-generated method stub
            return true;
        }
        
    }

    protected static class FutureQueue<V, T extends Future<V>> extends ForwardingQueue<T> {

        protected final Queue<T> delegate;

        protected FutureQueue() {
            this(AbstractActor.<T>newQueue());
        }
        
        protected FutureQueue(Queue<T> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public Queue<T> delegate() {
            return delegate;
        }
        
        @Override
        public boolean isEmpty() {
            return peek() == null;
        }
        
        @Override
        public T peek() {
            T next = super.peek();
            if ((next != null) && next.isDone()) {
                return next;
            } else {
                return null;
            }
        }
        
        @Override
        public synchronized T poll() {
            T next = peek();
            if (next != null) {
                return super.poll();
            } else {
                return null;
            }
        }
        
        @Override
        public void clear() {
            T next;
            while ((next = super.poll()) != null) {
                next.cancel(true);
            }
        }
    }
    
}
