package edu.uw.zookeeper.safari.frontend;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Provider;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.LinkedQueue;
import edu.uw.zookeeper.safari.common.OperationFuture;
import edu.uw.zookeeper.safari.data.Volume;
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.server.AbstractSessionExecutor;

public class FrontendSessionExecutor extends AbstractSessionExecutor<ShardedResponseMessage<IWatcherEvent>> implements Processors.UncheckedProcessor<Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>, Message.ServerResponse<?>> {
    
    public static ParameterizedFactory<Session, FrontendSessionExecutor> factory(
             final Provider<? extends Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>>> processor,
             final CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
             final CachedFunction<Identifier, Identifier> assignmentLookup,
             final AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
             final ScheduledExecutorService scheduler,
             final Executor executor) {
        return new ParameterizedFactory<Session, FrontendSessionExecutor>() {
            @Override
            public FrontendSessionExecutor get(Session value) {
                return newInstance(
                        value, 
                        processor.get(),
                        volumeLookup,
                        assignmentLookup,
                        dispatchers,
                        scheduler,
                        executor);
            }
        };
    }
    
    public static FrontendSessionExecutor newInstance(
            Session session,
            Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor,
            CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
            CachedFunction<Identifier, Identifier> assignmentLookup,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            ScheduledExecutorService scheduler,
            Executor executor) {
        Automaton<ProtocolState,ProtocolState> state = Automatons.createSynchronized(
                Automatons.createSimple(
                        ProtocolState.CONNECTED));
        IConcurrentSet<SessionListener> listeners = new StrongConcurrentSet<SessionListener>();
        return new FrontendSessionExecutor(processor, volumeLookup, assignmentLookup, dispatchers, executor, session, state, listeners, scheduler);
    }
    
    public static interface FrontendRequestFuture extends OperationFuture<Message.ServerResponse<?>> {}

    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final ClientPeerConnectionExecutorsListener backends;
    protected final ShardingProcessor sharder;
    protected final SubmitProcessor submitter;
    protected final Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor;
    protected final FrontendSessionActor actor;

    public FrontendSessionExecutor(
            Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor,
            CachedFunction<ZNodeLabel.Path, Volume> volumes,
            CachedFunction<Identifier, Identifier> assignments,
            AsyncFunction<Identifier, ClientPeerConnectionDispatcher> dispatchers,
            Executor executor,
            Session session,
            Automaton<ProtocolState,ProtocolState> state,
            IConcurrentSet<SessionListener> listeners,
            ScheduledExecutorService scheduler) {
        super(session, state, listeners, scheduler);
        this.logger = LogManager.getLogger(getClass());
        this.processor = processor;
        this.actor = new FrontendSessionActor(executor);
        this.backends = ClientPeerConnectionExecutorsListener.newInstance(this, dispatchers, executor);
        this.sharder = new ShardingProcessor(volumes);
        this.submitter = new SubmitProcessor(assignments);
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Message.ClientRequest<?> request) {
        timer.send(request);
        if (request.xid() == OpCodeXid.PING.xid()) {
            // short circuit pings
            return Futures.<Message.ServerResponse<?>>immediateFuture(
                    apply(Pair.create(Optional.<Operation.ProtocolRequest<?>>of(request), 
                                    (Records.Response) Records.newInstance(IPingResponse.class))));
        } else {
            Promise<Message.ServerResponse<?>> promise = 
                    LoggingPromise.create(logger, 
                            SettableFuturePromise.<Message.ServerResponse<?>>create());
            FrontendRequestTask task = new FrontendRequestTask(request, promise);
            if (! actor.send(task)) {
                task.cancel(true);
            }
            return task;
        }
    }

    @Override
    public Message.ServerResponse<?> apply(Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response> input) {
        return processor.apply(Pair.create(session.id(), input));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onSuccess(ShardedResponseMessage<IWatcherEvent> result) {
        handleNotification((Message.ServerResponse<IWatcherEvent>) apply(
                Pair.create( 
                        Optional.<Operation.ProtocolRequest<?>>absent(),
                        (Records.Response) result.record()))); 
    }

    protected class FrontendSessionActor extends ExecutedActor<FrontendRequestTask> {

        protected final Executor executor;
        protected final LinkedQueue<FrontendRequestTask> mailbox;
        
        public FrontendSessionActor(
                Executor executor) {
            this.executor = executor;
            this.mailbox = LinkedQueue.create();
        }
        
        /**
         * Not thread safe
         */
        @Override
        protected synchronized boolean doSend(FrontendRequestTask message) {
            if (! mailbox().offer(message)) {
                return false;
            }
            if (!sharder.send(message) || (!schedule() && (state() == State.TERMINATED))) {
                mailbox().remove(message);
                return false;
            }
            return true;
        }

        @Override
        protected boolean schedule() {
            FrontendRequestTask next = mailbox.peek();
            if ((next != null) && (next.responses().isDone())) {
                return super.schedule();
            } else {
                return false;
            }
        }

        @Override
        protected void doRun() throws Exception {
            FrontendRequestTask next;
            while ((next = mailbox.peek()) != null) {
                logger().debug("Applying {} ({})", next, this);
                if (!apply(next)) {
                    break;
                }
            }
        }
        
        @Override
        protected synchronized boolean apply(FrontendRequestTask input) throws Exception {
            if (input.responses().isDone()) {
                Records.Response response = input.responses().get();
                if (mailbox.remove(input)) {
                    Message.ServerResponse<?> result = 
                            FrontendSessionExecutor.this.apply( 
                                    Pair.create(
                                            Optional.<Operation.ProtocolRequest<?>>of(input.task()), 
                                            response));
                    input.set(result);
                    return true;
                }
            }
            return (state() != State.TERMINATED);
        }
        
        @Override
        protected synchronized void doStop() {
            sharder.stop();
            submitter.stop();
            FrontendRequestTask next;
            while ((next = mailbox.poll()) != null) {
                next.cancel(true);
            }
            backends.stop();
        }

        @Override
        protected Executor executor() {
            return executor;
        }

        @Override
        protected LinkedQueue<FrontendRequestTask> mailbox() {
            return mailbox;
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }

    public static <T extends Records.Request> T validate(Volume volume, T request) throws KeeperException {
        switch (request.opcode()) {
        case CREATE:
        case CREATE2:
        {
            // special case: root of a volume can't be sequential!
            Records.CreateModeGetter create = (Records.CreateModeGetter) request;
            if (CreateMode.valueOf(create.getFlags()).contains(CreateFlag.SEQUENTIAL)
                    && volume.getDescriptor().getRoot().toString().equals(create.getPath())) {
                // fail
                throw new KeeperException.BadArgumentsException(create.getPath());
            }
        }
        default:
            break;
        }
        return request;
    }

    protected class FrontendRequestTask extends PromiseTask<Message.ClientRequest<?>, Message.ServerResponse<?>> 
            implements Runnable {

        protected volatile VolumesTask volumes;
        protected volatile ResponsesTask responses;
        
        public FrontendRequestTask(
                Message.ClientRequest<?> task,
                Promise<Message.ServerResponse<?>> promise) {
            super(task, promise);
            this.volumes = new VolumesTask(
                    LoggingPromise.create(logger, 
                            SettableFuturePromise.<Set<Volume>>create()));
            this.responses = new ResponsesTask();
            this.responses.addListener(this, SAME_THREAD_EXECUTOR);
        }
        
        public VolumesTask volumes() {
            return volumes;
        }
        
        public ResponsesTask responses() {
            return responses;
        }

        @Override
        public void run() {
            if (responses.isDone()) {
                if (this == actor.mailbox().peek()) {
                    actor.run();
                }
            }
        }

        @Override
        public boolean set(Message.ServerResponse<?> result) {
            assert ((result.record().opcode() == task.record().opcode()) ||
                    (result.record() instanceof Operation.Error));
            return super.set(result);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancel = super.cancel(mayInterruptIfRunning);
            if (cancel) {
                synchronized (this) {
                    volumes.cancel(mayInterruptIfRunning);
                    responses.cancel(mayInterruptIfRunning);
                }
            }
            return cancel;
        }

        @Override
        public boolean setException(Throwable t) {
            boolean setException = super.setException(t);
            if (setException) {
                synchronized (this) {
                    volumes.cancel(false);
                    responses.cancel(false);
                }
            }
            return setException;
        }
        
        @Override
        protected synchronized Objects.ToStringHelper toString(Objects.ToStringHelper toString) {
            return super.toString(toString).add("volumes", volumes).add("responses", responses);
        }
        
        protected class VolumesTask extends ForwardingPromise<Set<Volume>> implements AsyncFunction<AsyncFunction<ZNodeLabel.Path, Volume>, Set<Volume>> {

            protected final Set<ListenableFuture<Volume>> pending;
            protected final Promise<Set<Volume>> promise;
            
            public VolumesTask(Promise<Set<Volume>> promise) {
                this.promise = promise;
                this.pending = Sets.newHashSet();
            }
            
            public Set<ListenableFuture<Volume>> pending() {
                return ImmutableSet.copyOf(pending);
            }

            @Override
            public synchronized ListenableFuture<Set<Volume>> apply(AsyncFunction<ZNodeLabel.Path, Volume> lookup) throws Exception {
                if (! isDone()) {
                    if (pending.isEmpty()) {
                        ZNodeLabel.Path[] paths = PathsOfRequest.getPathsOfRequest(task.record());
                        if (paths.length == 0) {
                            set(ImmutableSet.<Volume>of());
                        } else {
                            for (int i=0; i<paths.length; ++i) {
                                ListenableFuture<Volume> future = lookup.apply(paths[i]);
                                pending.add(future);
                            }
                        }
                    } else {
                        ImmutableSet.Builder<Volume> builder = ImmutableSet.builder();
                        for (ListenableFuture<Volume> next: pending) {
                            if (next.isDone()) {
                                builder.add(next.get());
                            } else {
                                break;
                            }
                        }
                        ImmutableSet<Volume> volumes = builder.build();
                        if (volumes.size() == pending.size()) {
                            pending.clear();
                            set(volumes);
                        }
                    }
                }
                return this;
            }
            
            @Override
            public String toString() {
                Objects.ToStringHelper toString = Objects.toStringHelper(this);
                if (isDone()) {
                    try {
                        toString.addValue(get());
                    } catch (Exception e) {
                        toString.addValue(e);
                    }
                }
                return toString.toString();
            }
            
            @Override
            protected Promise<Set<Volume>> delegate() {
                return promise;
            }
        }

        protected class ShardedRequestTask extends PromiseTask<Pair<Volume, ? extends ShardedRequestMessage<?>>, ShardedResponseMessage<?>> implements FutureCallback<ShardedResponseMessage<?>> {

            protected volatile ListenableFuture<ShardedResponseMessage<?>> future;
            
            public ShardedRequestTask(Volume volume, Message.ClientRequest<?> request) {
                this(volume, ShardedRequestMessage.of(volume.getId(), request), LoggingPromise.create(logger, SettableFuturePromise.<ShardedResponseMessage<?>>create()));
                this.future = null;
            }
            
            public ShardedRequestTask(Volume volume, ShardedRequestMessage<?> request, 
                    Promise<ShardedResponseMessage<?>> promise) {
                super(Pair.create(volume, request), promise);
            }

            public Volume volume() {
                return task.first();
            }
            
            public ShardedRequestMessage<?> request() {
                return task.second();
            }
            
            public synchronized ListenableFuture<ShardedResponseMessage<?>> apply(TaskExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>> executor) {
                this.future = executor.submit(request());
                Futures.addCallback(future, this, SAME_THREAD_EXECUTOR);
                return this;
            }

            @Override
            public void onSuccess(ShardedResponseMessage<?> result) {
                set(result);
            }

            @Override
            public void onFailure(Throwable t) {
                setException(t);
            }
        }
        
        protected class ResponsesTask extends RunnablePromiseTask<Map<Volume, ShardedRequestTask>, Records.Response> implements Runnable, Function<Set<Volume>, Map<Volume, ShardedRequestTask>> {

            public ResponsesTask() {
                this(Maps.<Volume, ShardedRequestTask>newHashMapWithExpectedSize(1), 
                        LoggingPromise.create(logger, SettableFuturePromise.<Records.Response>create()));
            }

            public ResponsesTask(Map<Volume, ShardedRequestTask> shards, Promise<Records.Response> promise) {
                super(shards, promise);
            }
            
            // TODO: thread-safety
            public Map<Volume, ShardedRequestTask> shards() {
                return task;
            }

            @Override
            public synchronized Map<Volume, ShardedRequestTask> apply(Set<Volume> volumes) {
                if (volumes.isEmpty()) {
                    return task;
                }
                Sets.SetView<Volume> difference = Sets.difference(volumes, task.keySet());
                if (difference.isEmpty()) {
                    return task;
                }
                try {
                    if (OpCode.MULTI == FrontendRequestTask.this.task.record().opcode()) {
                        List<Pair<Volume, List<Records.MultiOpRequest>>> byShardOps = Lists.newArrayListWithCapacity(difference.size());
                        IMultiRequest multi = (IMultiRequest) FrontendRequestTask.this.task.record();
                        for (Records.MultiOpRequest op: multi) {
                            ZNodeLabel.Path[] paths = PathsOfRequest.getPathsOfRequest(op);
                            assert (paths.length > 0);
                            for (ZNodeLabel.Path path: paths) {
                                List<Records.MultiOpRequest> ops = null;
                                Volume v = null;
                                for (Pair<Volume, List<Records.MultiOpRequest>> e: byShardOps) {
                                    if (e.first().getDescriptor().contains(path)) {
                                        v = e.first();
                                        ops = e.second();
                                        break;
                                    }
                                }
                                if (ops == null) {
                                    ops = Lists.newArrayListWithCapacity(multi.size());
                                    for (Volume e: difference) {
                                        if (e.getDescriptor().contains(path)) {
                                            v = e;
                                            break;
                                        }
                                    }
                                    byShardOps.add(Pair.create(v, ops));
                                }
                                if (v != null) {
                                    ops.add(validate(v, op));
                                }
                            }
                        }
                        for (Pair<Volume, List<Records.MultiOpRequest>> e: byShardOps) {
                            ShardedRequestTask request = new ShardedRequestTask(
                                    e.first(), 
                                    ProtocolRequestMessage.of(
                                            FrontendRequestTask.this.task.xid(),
                                            new IMultiRequest(e.second())));
                            task.put(request.volume(), request);
                            request.addListener(this, SAME_THREAD_EXECUTOR);
                        }
                    } else {
                        for (Volume volume: difference) {
                            validate(volume, FrontendRequestTask.this.task.record());
                            ShardedRequestTask request = new ShardedRequestTask(
                                    volume, FrontendRequestTask.this.task);
                            task.put(request.volume(), request);
                            request.addListener(this, SAME_THREAD_EXECUTOR);
                        }
                    }
                } catch (Exception e) {
                    setException(e);
                }
                
                return task;
            }

            @Override
            public synchronized Optional<Records.Response> call() throws Exception {
                for (ShardedRequestTask request: task.values()) {
                    if (! request.isDone()) {
                        return Optional.absent();
                    }
                }
                
                Records.Response result = null;
                if (OpCode.MULTI == FrontendRequestTask.this.task.record().opcode()) {
                    Map<Volume, ListIterator<Records.MultiOpResponse>> opResponses = Maps.newHashMapWithExpectedSize(task.size());
                    Map<Volume, ListIterator<Records.MultiOpRequest>> opRequests = Maps.newHashMapWithExpectedSize(task.size());
                    for (ShardedRequestTask e: task.values()) {
                        opRequests.put(e.volume(), ((IMultiRequest) e.get().record()).listIterator());
                        opResponses.put(e.volume(), ((IMultiResponse) e.get().record()).listIterator());
                    }
                    IMultiRequest multi = (IMultiRequest) FrontendRequestTask.this.task.record();
                    List<Records.MultiOpResponse> ops = Lists.newArrayListWithCapacity(multi.size());
                    for (Records.MultiOpRequest op: multi) {
                        Pair<Volume, Records.MultiOpResponse> response = null;
                        for (Map.Entry<Volume, ListIterator<Records.MultiOpRequest>> request: opRequests.entrySet()) {
                            if (! request.getValue().hasNext()) {
                                continue;
                            }
                            if (! op.equals(request.getValue().next())) {
                                request.getValue().previous();
                                continue;
                            }
                            Volume volume = request.getKey();
                            if ((response == null) 
                                    || (response.first().getDescriptor().getRoot().prefixOf(
                                            volume.getDescriptor().getRoot()))) {
                                response = Pair.create(volume, opResponses.get(volume).next());
                                break;
                            }
                        }
                        assert (response != null);
                        ops.add(response.second());
                    }
                    result = new IMultiResponse(ops);
                } else {
                    Pair<Volume, ? extends ShardedResponseMessage<?>> selected = null;
                    for (ShardedRequestTask e: task.values()) {
                        ShardedResponseMessage<?> response = e.get();
                        if (selected == null) {
                            selected = Pair.create(e.volume(), response);
                        } else if (FrontendRequestTask.this.task.record().opcode() == OpCode.CLOSE_SESSION) {
                            // pick the error response for now
                            if (response.record() instanceof Operation.Error) {
                                selected = Pair.create(e.volume(), response);
                            }
                        } else {
                            if (((selected.second().record() instanceof Operation.Error) ^ (response.record() instanceof Operation.Error))
                                    || ((response.record() instanceof Operation.Error) && (((Operation.Error) response.record()).error() != ((Operation.Error) selected.second().record()).error()))) {
                                throw new UnsupportedOperationException();
                            }
                            // we should only get here for create, create2,
                            // and delete
                            // and only if the path is for a volume root
                            // in that case, pick the response that came
                            // from the volume root
                            if (selected.first().getDescriptor().getRoot()
                                    .prefixOf(e.volume().getDescriptor().getRoot())) {
                                selected = Pair.create(e.volume(), response);
                            }
                        }
                    }
                    assert (selected != null);
                    result = selected.second().record();
                }
                return Optional.of(result);
            }
        }
    }
    
    protected class ShardingProcessor extends ExecutedActor<FrontendRequestTask> {

        protected final CachedFunction<ZNodeLabel.Path, Volume> lookup;
        protected final Queue<FrontendRequestTask> mailbox;
        // not thread-safe
        protected final Set<ListenableFuture<?>> futures;
        
        public ShardingProcessor(
                CachedFunction<ZNodeLabel.Path, Volume> lookup) {
            this.lookup = lookup;
            this.mailbox = Queues.newConcurrentLinkedQueue();
            this.futures =
                    Collections.newSetFromMap(
                    new WeakHashMap<ListenableFuture<?>, Boolean>());
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(FrontendSessionExecutor.this.toString()).toString();
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
            FrontendRequestTask next;
            while ((next = mailbox.peek()) != null) {
                logger().debug("Applying {} ({})", next, this);
                if (! apply(next)) {
                    break;
                }
            }
        }
        
        @Override
        protected synchronized boolean apply(FrontendRequestTask input) throws Exception {
            ListenableFuture<Set<Volume>> future = input.volumes().apply(lookup);
            if (future.isDone()) {
                mailbox.remove(input);
                input.responses().apply(future.get());
                submitter.send(input);
                return true;
            } else {
                for (ListenableFuture<Volume> lookup: input.volumes().pending()) {
                    if (! lookup.isDone()) {
                        if (futures.add(lookup)) {
                            lookup.addListener(this, SAME_THREAD_EXECUTOR);
                        }
                    }
                }
                return false;
            }
        }

        @Override
        protected Queue<FrontendRequestTask> mailbox() {
            return mailbox;
        }

        @Override
        protected Executor executor() {
            return FrontendSessionExecutor.this.actor.executor();
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }

    protected static interface VolumeProcessor extends Actor<Promise<?>> {
        Volume volume();
    }
    
    protected class SubmitProcessor extends ExecutedActor<FrontendRequestTask> {

        protected final Queue<FrontendRequestTask> mailbox;
        protected final CachedFunction<Identifier, Identifier> assignments;
        protected final ConcurrentMap<Volume, VolumeProcessor> processors;
        // not thread-safe
        protected final Set<ListenableFuture<?>> futures;
        
        public SubmitProcessor(
                CachedFunction<Identifier, Identifier> assignments) {
            this.mailbox = Queues.newConcurrentLinkedQueue();
            this.assignments = assignments;
            this.processors = new MapMaker().makeMap();
            this.futures = Sets.newHashSet();
        }

        public ListenableFuture<ClientPeerConnectionExecutor> lookup(Volume volume) throws Exception {
            ListenableFuture<Identifier> assignment = assignments.apply(volume.getId());
            ListenableFuture<ClientPeerConnectionExecutor> connection = 
                    Futures.transform(assignment, backends.asLookup(), SAME_THREAD_EXECUTOR);
            if (logger.isTraceEnabled()) {
                if (! connection.isDone()) {
                    logger.trace("Waiting for connection for {}", volume);
                }
            }
            return connection;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(FrontendSessionExecutor.this.toString()).toString();
        }

        @Override
        protected synchronized boolean schedule() {
            for (ListenableFuture<?> future : futures) {
                if (! future.isDone()) {
                    return false;
                }
            }
            return super.schedule();
        }
        
        @Override
        protected void doRun() throws Exception {
            FrontendRequestTask next;
            while ((next = mailbox().peek()) != null) {
                logger().debug("Applying {} ({})", next, this);
                if (apply(next)) {
                    mailbox().remove(next);
                } else {
                    break;
                }
            }
        }

        @Override
        protected synchronized boolean apply(FrontendRequestTask input) throws KeeperException {
            if (input.responses().shards().isEmpty()) {
                assert (input.task().record().opcode() == OpCode.CLOSE_SESSION);
                
                if (futures.isEmpty()) {
                    List<ListenableFuture<?>> flushed = Lists.newArrayListWithCapacity(processors.size());
                    for (Map.Entry<Volume, VolumeProcessor> e: processors.entrySet()) {
                        FlushPromise f = new FlushPromise();
                        if (! e.getValue().send(f)) {
                            return false;
                        }
                        flushed.add(f);
                    }
                    if (flushed.isEmpty()) {
                        // special case - empty session
                        input.responses().apply(ImmutableSet.of(Volume.none()));
                        return apply(input);
                    } else {
                        ListenableFuture<List<Object>> all = Futures.allAsList(flushed);
                        futures.add(all);
                        all.addListener(this, SAME_THREAD_EXECUTOR);
                        return false;
                    }
                } else {
                    assert (Iterables.getOnlyElement(futures).isDone());
                    futures.clear();
                    
                    Map<Identifier, Volume> volumePerRegion = Maps.newHashMapWithExpectedSize(backends.asCache().values().size());
                    for (VolumeProcessor e: processors.values()) {
                        if (e instanceof ConnectionVolumeProcessor) {
                            ConnectionVolumeProcessor processor = (ConnectionVolumeProcessor) e;
                            Identifier region = processor.connection().dispatcher().getIdentifier();
                            if (!volumePerRegion.containsKey(region) && !volumePerRegion.containsValue(e.volume())) {
                                volumePerRegion.put(region, e.volume());
                            }
                        }
                    }
                    assert (volumePerRegion.size() == backends.asCache().values().size());
                    input.responses().apply(ImmutableSet.copyOf(volumePerRegion.values()));
                    return apply(input);
                }
            } else {
                for (FrontendRequestTask.ShardedRequestTask shard: input.responses().shards().values()) {
                    VolumeProcessor processor = processors.get(shard.volume());
                    if (processor == null) {
                        if (shard.volume().equals(Volume.none())) {
                            processor = new NoneVolumeProcessor();
                        } else {
                            processor = new ConnectionVolumeProcessor(shard.volume());
                            ListenableFuture<ClientPeerConnectionExecutor> connection;
                            try {
                                connection = lookup(processor.volume());
                            } catch (Exception e) {
                                // TODO
                                throw Throwables.propagate(e);
                            }
                            Futures.addCallback(connection, (ConnectionVolumeProcessor) processor, SAME_THREAD_EXECUTOR);
                        }
                        processors.put(processor.volume(), processor);
                    }
                    if (!processor.send(shard)) {
                        return false;
                    }
                }
            }
            return true;
        }
        
        @Override
        protected void doStop() {
            for (VolumeProcessor processor: processors.values()) {
                processor.stop();
            }
            
            super.doStop();
        }

        @Override
        protected Logger logger() {
            return logger;
        }

        @Override
        protected Executor executor() {
            return FrontendSessionExecutor.this.actor.executor();
        }

        @Override
        protected Queue<FrontendRequestTask> mailbox() {
            return mailbox;
        }
        
        protected class FlushPromise extends ForwardingPromise<VolumeProcessor> {

            protected final Promise<VolumeProcessor> promise;
            
            public FlushPromise() {
                this(LoggingPromise.create(logger(), SettableFuturePromise.<VolumeProcessor>create()));
            }

            public FlushPromise(Promise<VolumeProcessor> promise) {
                this.promise = promise;
            }
            
            @Override
            protected Promise<VolumeProcessor> delegate() {
                return promise;
            }
        }

        protected class NoneVolumeProcessor extends AbstractActor<Promise<?>> implements VolumeProcessor {

            public NoneVolumeProcessor() {
            }

            @Override
            public Volume volume() {
                return Volume.none();
            }

            @Override
            public String toString() {
                return Objects.toStringHelper(this)
                        .addValue(SubmitProcessor.this.toString()).toString();
            }

            @Override
            protected boolean doSend(Promise<?> message) {
                if (message instanceof FrontendRequestTask.ShardedRequestTask) {
                    FrontendRequestTask.ShardedRequestTask request = (FrontendRequestTask.ShardedRequestTask) message;
                    request.set(ShardedResponseMessage.of(
                            request.request().getIdentifier(),
                            ProtocolResponseMessage.of(
                                    request.request().xid(), 0L,
                                    Records.Responses.getInstance().get(request.request().record().opcode()))));
                } else if (message instanceof FlushPromise) {
                    ((FlushPromise) message).set(this);
                } else {
                    throw new AssertionError(String.valueOf(message));
                }
                return true;
            }

            @Override
            protected void doRun() throws Exception {
            }

            @Override
            protected void doStop() {
            }

            @Override
            protected Logger logger() {
                return logger;
            }
        }
        
        protected class ConnectionVolumeProcessor extends ExecutedActor<Promise<?>> implements VolumeProcessor, FutureCallback<ClientPeerConnectionExecutor> {

            protected final Volume volume;
            protected final Queue<Promise<?>> mailbox;
            // not thread-safe
            protected volatile ClientPeerConnectionExecutor connection;
            
            public ConnectionVolumeProcessor(
                    Volume volume) {
                this.volume = volume;
                this.mailbox = Queues.newConcurrentLinkedQueue();
                this.connection = null;
            }
            
            public ClientPeerConnectionExecutor connection() {
                return connection;
            }

            @Override
            public Volume volume() {
                return volume;
            }

            @Override
            public void onSuccess(ClientPeerConnectionExecutor result) {
                this.connection = result;
                run();
            }

            @Override
            public void onFailure(Throwable t) {
                // FIXME
                throw new AssertionError(t);
            }

            @Override
            public String toString() {
                return Objects.toStringHelper(this)
                        .add("volume", volume)
                        .addValue(SubmitProcessor.this.toString()).toString();
            }

            @Override
            protected boolean schedule() {
                if (connection == null) {
                    return false;
                } else {
                    return super.schedule();
                }
            }
            
            @Override
            protected void doRun() throws Exception {
                Promise<?> next;
                while ((next = mailbox().peek()) != null) {
                    logger().debug("Applying {} ({})", next, this);
                    if (! apply(next)) {
                        break;
                    }
                }
            }

            @Override
            protected boolean apply(Promise<?> input) throws Exception {
                ClientPeerConnectionExecutor connection = this.connection;
                if (connection == null) {
                    return false;
                } else if (connection.state() == State.TERMINATED) {
                    this.connection = null;
                    // FIXME
                    throw new UnsupportedOperationException();
                }
                if (input instanceof FrontendRequestTask.ShardedRequestTask) {
                    FrontendRequestTask.ShardedRequestTask request = (FrontendRequestTask.ShardedRequestTask) input;
                    mailbox.remove(input);
                    request.apply(connection);
                } else if (input instanceof FlushPromise) {
                    mailbox.remove(input);
                    ((FlushPromise) input).set(this);
                } else {
                    throw new AssertionError(String.valueOf(input));
                }
                return true;
            }

            @Override
            protected Executor executor() {
                return SubmitProcessor.this.executor();
            }

            @Override
            protected Queue<Promise<?>> mailbox() {
                return mailbox;
            }

            @Override
            protected Logger logger() {
                return logger;
            }   
        }
    }
}