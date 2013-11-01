package edu.uw.zookeeper.safari.frontend;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Provider;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
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

public class FrontendSessionExecutor extends AbstractSessionExecutor<ShardedResponseMessage<IWatcherEvent>> {
    
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
    
    @SuppressWarnings("unchecked")
    @Override
    public void onSuccess(ShardedResponseMessage<IWatcherEvent> result) {
        handleNotification((Message.ServerResponse<IWatcherEvent>) apply(
                Pair.create( 
                        Optional.<Operation.ProtocolRequest<?>>absent(),
                        (Records.Response) result.record()))); 
    }
    
    public Message.ServerResponse<?> apply(Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response> input) {
        return processor.apply(Pair.create(session.id(), input));
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Message.ClientRequest<?> request) {
        timer.send(request);
        if (request.xid() == OpCodeXid.PING.xid()) {
            // short circuit pings
            return Futures.<Message.ServerResponse<?>>immediateFuture(processor.apply(
                    Pair.create(session().id(), 
                            Pair.create(Optional.<Operation.ProtocolRequest<?>>of(request), 
                                    (Records.Response) Records.newInstance(IPingResponse.class)))));
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

    protected class FrontendSessionActor extends ExecutedActor<FrontendRequestTask> {

        protected final Executor executor;
        protected final LinkedQueue<FrontendRequestTask> mailbox;
        
        public FrontendSessionActor(
                Executor executor) {
            this.executor = executor;
            this.mailbox = LinkedQueue.create();
        }
        
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
        protected void doRun() throws Exception {
            FrontendRequestTask next;
            while ((next = mailbox.peek()) != null) {
                if (!apply(next)) {
                    break;
                }
            }
        }
        
        @Override
        protected synchronized boolean apply(FrontendRequestTask input) throws Exception {
            if (state() != State.TERMINATED) {
                Records.Response response = input.call();
                if (response != null) {
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
            }
            return false;
        }
        
        @Override
        protected void runExit() {
            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                FrontendRequestTask next = mailbox.peek();
                try {
                    if ((next != null) && (next.call() != null)) {
                        schedule();
                    }
                } catch (Exception e) {
                    // TODO
                    throw new AssertionError(e);
                }
            }
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
            implements Runnable, Callable<Records.Response> {

        protected volatile List<Pair<Volume, ShardedRequestMessage<?>>> shards;
        protected volatile List<Pair<?, ? extends ListenableFuture<?>>> pending;
        
        public FrontendRequestTask(
                Message.ClientRequest<?> task,
                Promise<Message.ServerResponse<?>> promise) {
            super(task, promise);
            this.shards = ImmutableList.of();
            this.pending = Lists.newLinkedList();
        }

        public Pair<ZNodeLabel.Path[], ListenableFuture<Volume>[]> lookup(AsyncFunction<ZNodeLabel.Path, Volume> lookup) throws Exception {
            ZNodeLabel.Path[] paths = PathsOfRequest.getPathsOfRequest(task.record());
            @SuppressWarnings("unchecked")
            ListenableFuture<Volume>[] lookups = (ListenableFuture<Volume>[]) new ListenableFuture<?>[paths.length];
            for (int i=0; i<paths.length; ++i) {
                lookups[i] = lookup.apply(paths[i]);
            }
            return Pair.create(paths, lookups);
        }
        
        public List<Pair<Volume, ShardedRequestMessage<?>>> shards() {
            return shards;
        }

        public synchronized List<Pair<Volume, ShardedRequestMessage<?>>> shard(List<Pair<ZNodeLabel.Path, Volume>> volumes) throws KeeperException {
            if (volumes.isEmpty()) {
                Volume v = Volume.none();
                shards = ImmutableList.of(Pair.<Volume, ShardedRequestMessage<?>>create(v, ShardedRequestMessage.of(v.getId(), task)));
            } else {
                shards = Lists.newArrayListWithCapacity(volumes.size());
                if (OpCode.MULTI == task.record().opcode()) {
                    List<Pair<Volume, List<Records.MultiOpRequest>>> byShardOps = Lists.newArrayListWithCapacity(shards.size());
                    IMultiRequest multi = (IMultiRequest) task.record();
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
                                for (Pair<ZNodeLabel.Path, Volume> e: volumes) {
                                    if (e.first().equals(path)) {
                                        v = e.second();
                                        break;
                                    }
                                }
                                byShardOps.add(Pair.create(v, ops));
                            }
                            assert (v != null);
                            ops.add(validate(v, op));
                        }
                    }
                    for (Pair<Volume, List<Records.MultiOpRequest>> e: byShardOps) {
                        shards.add(Pair.<Volume, ShardedRequestMessage<?>>create(
                                e.first(), 
                                ShardedRequestMessage.of(
                                        e.first().getId(),
                                        ProtocolRequestMessage.of(
                                                task.xid(),
                                                new IMultiRequest(e.second())))));
                    }
                } else {
                    for (Pair<ZNodeLabel.Path, Volume> e: volumes) {
                        Volume v = e.second();
                        boolean unique = true;
                        for (Pair<Volume, ShardedRequestMessage<?>> shard: shards) {
                            if (shard.first().equals(v)) {
                                unique = false;
                                break;
                            }
                        }
                        if (! unique) {
                            continue;
                        }
                        validate(v, task.record());
                        shards.add(Pair.<Volume, ShardedRequestMessage<?>>create(
                                v, ShardedRequestMessage.of(v.getId(), task)));
                    }
                }
            }
            return shards;
        }
        
        public synchronized Records.Response unshard(List<Pair<?, ShardedResponseMessage<?>>> responses) {
            checkArgument(! responses.isEmpty());
            Records.Response result = null;
            if (OpCode.MULTI == task.record().opcode()) {
                List<Pair<Volume, ListIterator<Records.MultiOpResponse>>> opResponses = Lists.newArrayListWithCapacity(responses.size());
                for (Pair<?, ShardedResponseMessage<?>> e: responses) {
                    opResponses.add(Pair.<Volume, ListIterator<Records.MultiOpResponse>>create((Volume) e.first(), ((IMultiResponse) e.second().record()).listIterator()));
                }
                List<Pair<Volume, ListIterator<Records.MultiOpRequest>>> opRequests = Lists.newArrayListWithCapacity(shards.size());
                for (Pair<Volume, ShardedRequestMessage<?>> e: shards) {
                    opRequests.add(Pair.<Volume, ListIterator<Records.MultiOpRequest>>create(e.first(), ((IMultiRequest) e.second().record()).listIterator()));
                }
                IMultiRequest multi = (IMultiRequest) task().record();
                List<Records.MultiOpResponse> ops = Lists.newArrayListWithCapacity(multi.size());
                for (Records.MultiOpRequest op: multi) {
                    Pair<Volume, Records.MultiOpResponse> response = null;
                    for (Pair<Volume, ListIterator<Records.MultiOpRequest>> request: opRequests) {
                        if (! request.second().hasNext()) {
                            continue;
                        }
                        if (! op.equals(request.second().next())) {
                            request.second().previous();
                            continue;
                        }
                        Volume volume = request.first();
                        if ((response == null) 
                                || (response.first().getDescriptor().getRoot().prefixOf(
                                        volume.getDescriptor().getRoot()))) {
                            for (Pair<Volume, ListIterator<Records.MultiOpResponse>> e: opResponses) {
                                if (e.first().equals(volume)) {
                                    response = Pair.create(e.first(), e.second().next());
                                    break;
                                }
                            }
                            break;
                        }
                    }
                    assert (response != null);
                    ops.add(response.second());
                }
                result = new IMultiResponse(ops);
            } else {
                Pair<?, ShardedResponseMessage<?>> selected = null;
                for (Pair<?, ShardedResponseMessage<?>> e: responses) {
                    if (selected == null) {
                        selected = e;
                    } else if (task().record().opcode() == OpCode.CLOSE_SESSION) {
                        // pick the error response for now
                        if (e.second().record() instanceof Operation.Error) {
                            selected = e;
                        }
                    } else {
                        if (((selected.second().record() instanceof Operation.Error) ^ (e.second().record() instanceof Operation.Error))
                                || ((e.second().record() instanceof Operation.Error) && (((Operation.Error) e.second().record()).error() != ((Operation.Error) selected.second().record()).error()))) {
                            throw new UnsupportedOperationException();
                        }
                        // we should only get here for create, create2,
                        // and delete
                        // and only if the path is for a volume root
                        // in that case, pick the response that came
                        // from the volume root
                        if (((Volume) selected.first()).getDescriptor().getRoot()
                                .prefixOf(((Volume) e.first()).getDescriptor().getRoot())) {
                            selected = e;
                        }
                    }
                }
                assert (selected != null);
                result = selected.second().record();
            }
            assert (result != null);
            return result;
        }
        
        public List<Pair<?, ? extends ListenableFuture<?>>> pending() {
            return pending;
        }
        
        public synchronized void pending(Pair<?, ? extends ListenableFuture<?>> future) {
            int i=0;
            while (i < pending.size()) {
                if (pending.get(i).first().equals(future.first())) {
                    break;
                }
                i++;
            }
            if (i == pending.size()) {
                pending.add(future);
            } else {
                pending.set(i, future);
            }
            if (future.second() != null) {
                future.second().addListener(this, SAME_THREAD_EXECUTOR);
            }
        }

        @Override
        public void run() {
            if (! isPending()) {
                if (this == actor.mailbox().peek()) {
                    actor.run();
                }
            }
        }
        
        public synchronized boolean isPending() {
            for (Pair<?, ? extends ListenableFuture<?>> e: pending) {
                if ((e.second() == null) || !e.second().isDone()) {
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public synchronized Records.Response call() throws Exception {
            if (pending.isEmpty() || isPending()) {
                return null;
            }
            List<Pair<?, ShardedResponseMessage<?>>> results = Lists.newArrayListWithCapacity(pending.size());
            for (Pair<?, ? extends ListenableFuture<?>> e: pending) {
                if (e.second() != null) {
                    Object result = e.second().get();
                    if (result instanceof ShardedResponseMessage) {
                        results.add(Pair.<Object, ShardedResponseMessage<?>>create(e.first(), (ShardedResponseMessage<?>) result));
                    }
                }
            }
            if (! results.isEmpty()) {
                return unshard(results);
            } else {
                return null;
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
                    for (Pair<?, ? extends ListenableFuture<?>> e: pending) {
                        if (e.second() != null) {
                            e.second().cancel(mayInterruptIfRunning);
                        }
                    }
                }
            }
            return cancel;
        }

        @Override
        public boolean setException(Throwable t) {
            boolean setException = super.setException(t);
            if (setException) {
                synchronized (this) {
                    for (Pair<?, ? extends ListenableFuture<?>> e: pending) {
                        if (e.second() != null) {
                            e.second().cancel(false);
                        }
                    }
                }
            }
            return setException;
        }
        
        @Override
        protected synchronized Objects.ToStringHelper toString(Objects.ToStringHelper toString) {
            return super.toString(toString).add("shards", shards).add("pending", pending);
        }
    }
    
    protected static class ShardedRequest extends Pair<FrontendRequestTask, ShardedRequestMessage<?>> {
        public ShardedRequest(FrontendRequestTask first,
                ShardedRequestMessage<?> second) {
            super(first, second);
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
            this.futures = Collections.newSetFromMap(
                    new WeakHashMap<ListenableFuture<?>, Boolean>());
        }

        @Override
        protected boolean doSend(FrontendRequestTask message) {
            if (! mailbox().offer(message)) {
                return false;
            }
            if (state() == State.TERMINATED) {
                mailbox().remove(message);
                return false;
            }
            if (message == mailbox.peek()) {
                schedule();
            } else {
                try {
                    message.lookup(lookup);
                } catch (Exception e) {
                    // TODO
                    throw Throwables.propagate(e);
                }
            }
            return true;
        }
        
        @Override
        protected Executor executor() {
            return FrontendSessionExecutor.this.actor.executor();
        }

        @Override
        protected void doRun() throws Exception {
            FrontendRequestTask next;
            while ((next = mailbox.peek()) != null) {
                if (! apply(next)) {
                    break;
                }
            }
        }
        
        @Override
        protected boolean apply(FrontendRequestTask input) throws Exception {
            Pair<ZNodeLabel.Path[], ListenableFuture<Volume>[]> lookups = input.lookup(lookup);
            boolean complete = true;
            for (int i=0; i<lookups.first().length; ++i) {
                ListenableFuture<Volume> lookup = lookups.second()[i];
                if (! lookup.isDone()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Waiting for path lookup: {}", lookups.first()[i]);
                    }
                    if (futures.add(lookup)) {
                        lookup.addListener(this, SAME_THREAD_EXECUTOR);
                    }
                    complete = false;
                }
            }
            if (complete) {
                mailbox.remove(input);
                List<Pair<ZNodeLabel.Path, Volume>> volumes = Lists.newArrayListWithCapacity(lookups.first().length);
                for (int i=0; i<lookups.first().length; ++i) {
                    volumes.add(Pair.create(lookups.first()[i], lookups.second()[i].get()));
                }
                input.shard(volumes);
                submitter.send(input);
            }
            return complete;
        }

        @Override
        protected Queue<FrontendRequestTask> mailbox() {
            return mailbox;
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }
    
    protected class SubmitProcessor extends ExecutedActor<FrontendRequestTask> {

        protected final Queue<FrontendRequestTask> mailbox;
        protected final CachedFunction<Identifier, Identifier> assignments;
        protected final ConcurrentMap<Volume, VolumeProcessor> processors;
        
        public SubmitProcessor(
                CachedFunction<Identifier, Identifier> assignments) {
            this.mailbox = Queues.newConcurrentLinkedQueue();
            this.assignments = assignments;
            this.processors = new MapMaker().makeMap();
        }

        public ListenableFuture<ClientPeerConnectionExecutor> lookup(Volume volume) throws Exception {
            ListenableFuture<Identifier> assignment = assignments.apply(volume.getId());
            ListenableFuture<ClientPeerConnectionExecutor> backend = 
                    Futures.transform(assignment, backends.asLookup(), SAME_THREAD_EXECUTOR);
            if (logger.isTraceEnabled()) {
                if (! backend.isDone()) {
                    logger.trace("Waiting for backend for {}", volume);
                }
            }
            return backend;
        }

        @Override
        protected boolean schedule() {
            FrontendRequestTask next = mailbox.peek();
            if ((next == null) || next.isPending()) {
                return false;
            } else {
                return super.schedule();
            }
        }
        
        @Override
        protected void doRun() throws Exception {
            FrontendRequestTask next;
            while ((next = mailbox().peek()) != null) {
                synchronized (next) {
                    logger().debug("Applying {}", next);
                    if (apply(next)) {
                        mailbox().remove(next);
                    } else {
                        break;
                    }
                }
            }
        }

        @Override
        protected boolean apply(FrontendRequestTask input) {
            if (input.shards().get(0).first().equals(Volume.none())) {
                assert (input.shards().size() == 1);
                if (input.isPending()) {
                    return false;
                } else if (input.pending().isEmpty()) {
                    List<ListenableFuture<?>> flushed = Lists.newArrayListWithCapacity(processors.size());
                    for (Map.Entry<Volume, VolumeProcessor> e: processors.entrySet()) {
                        SettableFuturePromise<VolumeProcessor> f = SettableFuturePromise.create();
                        if (! e.getValue().send(f)) {
                            return false;
                        }
                        flushed.add(f);
                    }
                    if (flushed.isEmpty()) {
                        // special case
                        input.pending(Pair.create(Volume.none(), 
                                Futures.immediateFuture(ShardedResponseMessage.of(Identifier.zero(), ProtocolResponseMessage.of(
                                        input.task().xid(), 0L,
                                        Records.Responses.getInstance().get(input.task().record().opcode()))))));
                        return true;
                    } else {
                        ListenableFuture<List<Object>> all = Futures.allAsList(flushed);
                        input.pending(Pair.create(this, all));
                        all.addListener(this, SAME_THREAD_EXECUTOR);
                        return false;
                    }
                } else {
                    input.pending().clear();
                    for (ClientPeerConnectionExecutor backend: backends.asCache().values()) {
                        input.pending(Pair.create(backend, (ListenableFuture<?>) null));
                    }
                    for (ClientPeerConnectionExecutor backend: backends.asCache().values()) {
                        ListenableFuture<ShardedResponseMessage<?>> future = backend.submit(
                                        input.shards().get(0).second());
                        input.pending(Pair.create(backend, future));
                    }
                    assert(! input.pending().isEmpty());
                    return true;
                }
            }
            
            // create empty futures first!
            for (Pair<Volume, ShardedRequestMessage<?>> shard: input.shards()) {
                Volume volume = shard.first();
                assert (! volume.equals(Volume.none()));
                input.pending(Pair.create(volume, (ListenableFuture<?>) null));
            }
            // we assume that between iterating over the keys and values of processors
            // that the map doesn't change (probably a bad assumption)
            for (Pair<Volume, ShardedRequestMessage<?>> shard: input.shards()) {
                Volume volume = shard.first();
                assert (! volume.equals(Volume.none()));
                VolumeProcessor processor = processors.get(volume);
                if (processor == null) {
                    processor = new VolumeProcessor(volume);
                    if (processors.putIfAbsent(volume, processor) == null) {
                        ListenableFuture<ClientPeerConnectionExecutor> backend;
                        try {
                            backend = lookup(volume);
                        } catch (Exception e) {
                            // TODO
                            throw Throwables.propagate(e);
                        }
                        if (logger.isTraceEnabled()) {
                            if (! backend.isDone()) {
                                logger.trace("Waiting on connection for volume {}", volume);
                            }
                        }
                        Futures.addCallback(backend, processor, SAME_THREAD_EXECUTOR);
                    } else {
                        processor = processors.get(volume);
                    }
                }
                if (!processor.send(new ShardedRequest(input, shard.second()))) {
                    return false;
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

        protected class VolumeProcessor extends ExecutedActor<Object> implements FutureCallback<ClientPeerConnectionExecutor> {

            protected final Volume volume;
            protected final Queue<Object> mailbox;
            // not thread-safe
            protected volatile ClientPeerConnectionExecutor connection;
            
            public VolumeProcessor(
                    Volume volume) {
                this.volume = volume;
                this.mailbox = Queues.newConcurrentLinkedQueue();
                this.connection = null;
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
            protected boolean schedule() {
                if (connection == null) {
                    return false;
                } else {
                    return super.schedule();
                }
            }
            
            @Override
            protected void doRun() throws Exception {
                Object next;
                while ((next = mailbox().peek()) != null) {
                    logger().debug("Applying {}", next);
                    if (apply(next)) {
                        mailbox().remove(next);
                    } else {
                        break;
                    }
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            protected boolean apply(Object input) throws Exception {
                if (input instanceof ShardedRequest) {
                    ShardedRequest request = (ShardedRequest) input;
                    ClientPeerConnectionExecutor connection = this.connection;
                    if (connection == null) {
                        return false;
                    } else if (connection.state() == State.TERMINATED) {
                        this.connection = null;
                        // FIXME
                        throw new UnsupportedOperationException();
                    }
                    ListenableFuture<ShardedResponseMessage<?>> future = connection.submit(request.second());
                    request.first().pending(Pair.create(volume, future));
                } else if (input instanceof Promise<?>) {
                    ((Promise<? super VolumeProcessor>) input).set(this);
                }
                return true;
            }

            @Override
            protected Executor executor() {
                return SubmitProcessor.this.executor();
            }

            @Override
            protected Queue<Object> mailbox() {
                return mailbox;
            }

            @Override
            protected Logger logger() {
                return logger;
            }   
        }
    }
}