package edu.uw.zookeeper.safari.frontend;

import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Queues;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IPingResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.LinkedQueue;
import edu.uw.zookeeper.safari.common.OperationFuture;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.data.Volume;
import edu.uw.zookeeper.safari.peer.protocol.ClientPeerConnection;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class FrontendSessionExecutor extends ExecutedActor<FrontendSessionExecutor.FrontendRequestTask> implements TaskExecutor<Message.ClientRequest<?>, Message.ServerResponse<?>>, Publisher, FutureCallback<ShardedResponseMessage<?>> {
    
    public static FrontendSessionExecutor newInstance(
            Session session,
            Publisher publisher,
            Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor,
            CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
            CachedFunction<Identifier, Identifier> assignmentLookup,
            Function<? super Identifier, Identifier> ensembleForPeer,
            CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> connectionLookup,
            Executor executor) {
        return new FrontendSessionExecutor(session, publisher, processor, volumeLookup, assignmentLookup, ensembleForPeer, connectionLookup, executor);
    }
    
    public static interface FrontendRequestFuture extends OperationFuture<Message.ServerResponse<?>> {}

    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
    protected final Logger logger;
    protected final Executor executor;
    protected final LinkedQueue<FrontendRequestTask> mailbox;
    protected final Publisher publisher;
    protected final Session session;
    protected final ShardingProcessor sharder;
    protected final SubmitProcessor submitter;
    protected final Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor;

    public FrontendSessionExecutor(
            Session session,
            Publisher publisher,
            Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> processor,
            CachedFunction<ZNodeLabel.Path, Volume> volumes,
            CachedFunction<Identifier, Identifier> assignments,
            Function<? super Identifier, Identifier> ensembleForPeer,
            CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> connectionLookup,
            Executor executor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.executor = executor;
        this.mailbox = LinkedQueue.create();
        this.publisher = publisher;
        this.session = session;
        this.processor = processor;
        this.sharder = new ShardingProcessor(volumes);
        this.submitter = new SubmitProcessor(assignments, 
                        new BackendLookup(ensembleForPeer, connectionLookup));
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
    public void onSuccess(ShardedResponseMessage<?> result) {
        if (result.xid() == OpCodeXid.NOTIFICATION.xid()) {
            post(processor.apply(
                    Pair.create(session().id(),
                            Pair.create(
                                    Optional.<Operation.ProtocolRequest<?>>absent(),
                                    (Records.Response) result.record()))));
        }
    }
    
    @Override
    public void onFailure(Throwable t) {
        // FIXME
        throw new AssertionError(t);
    }
    
    public Session session() {
        return session;
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Message.ClientRequest<?> request) {
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
            if (! send(task)) {
                task.cancel(true);
            }
            return task;
        }
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

    @Subscribe
    public void handleTransition(Pair<Identifier, Automaton.Transition<?>> event) {
        if (state() == State.TERMINATED) {
            return;
        }
        Identifier ensemble = submitter.backends.getEnsembleForPeer().apply(event.first());
        BackendSessionExecutor backend = submitter.backends.asCache().get(ensemble);
        if (backend == null) {
            return;
        }
        backend.handleTransition(event.second());
        
        if (backend.state().compareTo(State.TERMINATED) >= 0) {
            run();
        }
    }

    @Subscribe
    public void handleResponse(Pair<Identifier, ShardedResponseMessage<?>> message) {
        if (state() == State.TERMINATED) {
            return;
        }
        Identifier ensemble = submitter.backends.getEnsembleForPeer().apply(message.first());
        BackendSessionExecutor backend = submitter.backends.asCache().get(ensemble);
        if (backend == null) {
            return;
        }
        backend.handleResponse(message.second());
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
                    Message.ServerResponse<?> result = processor.apply(
                            Pair.create(session().id(), 
                                    Pair.create(
                                            Optional.<Operation.ProtocolRequest<?>>of(input.task()), 
                                            response)));
                    post(result);
                    // set after post
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
        // FIXME: stop backends
        FrontendRequestTask next;
        while ((next = mailbox.poll()) != null) {
            next.cancel(true);
        }
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

    // TODO handle disconnects and reconnects
    protected class BackendLookup extends CachedLookup<Identifier, BackendSessionExecutor> {
    
        protected final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> connectionLookup;
        protected final Function<? super Identifier, Identifier> ensembleForPeer;
        
        public BackendLookup(
                Function<? super Identifier, Identifier> ensembleForPeer,
                CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> connectionLookup) {
            this(ensembleForPeer, connectionLookup, 
                    new MapMaker().<Identifier, BackendSessionExecutor>makeMap());
        }
        
        public BackendLookup(
                final Function<? super Identifier, Identifier> ensembleForPeer,
                final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket<?>>>> connectionLookup,
                final ConcurrentMap<Identifier, BackendSessionExecutor> cache) {
            super(cache, CachedFunction.<Identifier, BackendSessionExecutor>create(
                        new Function<Identifier, BackendSessionExecutor>() {
                             @Override
                             public BackendSessionExecutor apply(Identifier ensemble) {
                                 BackendSessionExecutor value = cache.get(ensemble);
                                 if ((value != null) && (value.state().compareTo(Actor.State.TERMINATED) < 0)) {
                                     return value;
                                 } else {
                                     return null;
                                 }
                             }
                        },
                        SharedLookup.create(
                            new AsyncFunction<Identifier, BackendSessionExecutor>() {
                                @Override
                                public ListenableFuture<BackendSessionExecutor> apply(Identifier ensemble) throws Exception {
                                    final BackendSessionExecutor prev = cache.get(ensemble);
                                    Optional<Session> session = (prev == null) ? 
                                            Optional.<Session>absent() : 
                                                Optional.of(prev.getSession());
                                    final EstablishBackendSessionTask task = EstablishBackendSessionTask.create(
                                            session(),
                                            session,
                                            ensemble, 
                                            connectionLookup.apply(ensemble),
                                            SettableFuturePromise.<Session>create());
                                    final FrontendSessionExecutor self = FrontendSessionExecutor.this;
                                    return Futures.transform(
                                            task, 
                                            new Function<Session, BackendSessionExecutor>() {
                                                @Override
                                                @Nullable
                                                public BackendSessionExecutor apply(Session input) {
                                                    BackendSessionExecutor backend = BackendSessionExecutor.create(
                                                            task.task(),
                                                            input,
                                                            Futures.getUnchecked(task.connection()),
                                                            self,
                                                            SAME_THREAD_EXECUTOR);
                                                    BackendSessionExecutor prev = cache.put(backend.getEnsemble(), backend);
                                                    if (prev != null) {
                                                        assert (prev.state() == Actor.State.TERMINATED);
                                                    }
                                                    return backend;
                                                }
                                            }, 
                                            SAME_THREAD_EXECUTOR);
                                }
                            })));
            this.ensembleForPeer = ensembleForPeer;
            this.connectionLookup = connectionLookup;
        }
        
        public Function<? super Identifier, Identifier> getEnsembleForPeer() {
            return ensembleForPeer;
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

    protected static final Pair<Volume, ListenableFuture<ShardedResponseMessage<?>>> EMPTY_RESPONSE = 
            Pair.<Volume, ListenableFuture<ShardedResponseMessage<?>>>create(Volume.none(), null);

    // future is set after message is published
    protected class FrontendRequestTask extends PromiseTask<Message.ClientRequest<?>, Message.ServerResponse<?>> 
            implements Runnable, Callable<Records.Response> {

        protected List<Pair<Volume, ShardedRequestMessage<?>>> shards;
        protected List<Pair<Volume, ListenableFuture<ShardedResponseMessage<?>>>> responses;
        
        public FrontendRequestTask(
                Message.ClientRequest<?> task,
                Promise<Message.ServerResponse<?>> promise) {
            super(task, promise);
            this.shards = ImmutableList.of();
            this.responses = ImmutableList.of();
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
            responses = Lists.newArrayListWithExpectedSize(shards.size());
            return shards;
        }
        
        public synchronized Records.Response unshard(List<Pair<Volume, ShardedResponseMessage<?>>> responses) {
            Records.Response result = null;
            if (OpCode.MULTI == task.record().opcode()) {
                List<Pair<Volume, ListIterator<Records.MultiOpResponse>>> opResponses = Lists.newArrayListWithCapacity(responses.size());
                for (Pair<Volume, ShardedResponseMessage<?>> e: responses) {
                    opResponses.add(Pair.<Volume, ListIterator<Records.MultiOpResponse>>create(e.first(), ((IMultiResponse) e.second().record()).listIterator()));
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
            } else if (responses.isEmpty()) {
                switch (task().record().opcode()) {
                case CLOSE_SESSION:
                    return Records.newInstance(IDisconnectResponse.class);
                default:
                    throw new AssertionError(this);
                }
            } else {
                Pair<Volume, ShardedResponseMessage<?>> selected = null;
                for (Pair<Volume, ShardedResponseMessage<?>> e: responses) {
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
                        if (selected.first().getDescriptor().getRoot()
                                .prefixOf(e.first().getDescriptor().getRoot())) {
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
        
        public synchronized void addResponse(Volume volume, ListenableFuture<ShardedResponseMessage<?>> future) {
            Pair<Volume, ListenableFuture<ShardedResponseMessage<?>>> value = Pair.<Volume, ListenableFuture<ShardedResponseMessage<?>>>create(volume, future);
            int i=0;
            while (i<responses.size()) {
                if (responses.get(i).first().equals(volume)) {
                    break;
                }
                i++;
            }
            if (i == responses.size()) {
                responses.add(value);
            } else {
                responses.set(i, value);
            }
            if (future != null) {
                future.addListener(this, SAME_THREAD_EXECUTOR);
            } else if (volume.equals(Volume.none())) {
                run();
            }
        }

        @Override
        public void run() {
            if (! isDone()) {
                Records.Response result;
                try {
                    result = call();
                } catch (Exception e) {
                    // FIXME
                    throw new AssertionError(e);
                }
                logger.trace("{} {}", result, this);
                if (result != null) {
                    if (this == mailbox.peek()) {
                        FrontendSessionExecutor.this.run();
                    }
                }
            }
        }
        
        @Override
        public synchronized Records.Response call() throws Exception {
            if (responses.isEmpty()) {
                return null;
            }
            for (Pair<Volume, ListenableFuture<ShardedResponseMessage<?>>> response: responses) {
                if (response.equals(EMPTY_RESPONSE)) {
                    return unshard(ImmutableList.<Pair<Volume, ShardedResponseMessage<?>>>of());
                } else if ((response.second() == null) || !response.second().isDone()) {
                    return null;
                }
            }
            List<Pair<Volume, ShardedResponseMessage<?>>> results = Lists.newArrayListWithCapacity(responses.size());
            for (Pair<Volume, ListenableFuture<ShardedResponseMessage<?>>> response: responses) {
                results.add(Pair.<Volume, ShardedResponseMessage<?>>create(response.first(), response.second().get()));
            }
            return unshard(results);
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
                    for (Pair<Volume, ListenableFuture<ShardedResponseMessage<?>>> e: responses) {
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
                    for (Pair<Volume, ListenableFuture<ShardedResponseMessage<?>>> e: responses) {
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
            return super.toString(toString).add("shards", shards).add("responses", responses);
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
            return FrontendSessionExecutor.this.executor();
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
                List<Pair<Volume, ShardedRequestMessage<?>>> shards = input.shard(volumes);
                submitter.send(Pair.create(input, shards));
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
    

    protected class SubmitProcessor extends AbstractActor<Pair<FrontendRequestTask, List<Pair<Volume, ShardedRequestMessage<?>>>>> {

        protected final CachedFunction<Identifier, Identifier> assignments;
        protected final BackendLookup backends;
        protected final ConcurrentMap<Volume, VolumeProcessor> processors;
        
        public SubmitProcessor(
                CachedFunction<Identifier, Identifier> assignments,
                BackendLookup backends) {
            this.assignments = assignments;
            this.backends = backends;
            this.processors = new MapMaker().makeMap();
        }

        public ListenableFuture<BackendSessionExecutor> lookup(Volume volume) throws Exception {
            ListenableFuture<Identifier> assignment = assignments.apply(volume.getId());
            ListenableFuture<BackendSessionExecutor> backend = Futures.transform(assignment, backends.asLookup(), SAME_THREAD_EXECUTOR);
            if (logger.isTraceEnabled()) {
                if (! backend.isDone()) {
                    logger.trace("Waiting for backend for {}", volume);
                }
            }
            return backend;
        }
        
        @Override
        protected boolean doSend(Pair<FrontendRequestTask, List<Pair<Volume, ShardedRequestMessage<?>>>> message) {
            // create empty futures first!
            for (Pair<Volume, ShardedRequestMessage<?>> shard: message.second()) {
                Volume volume = shard.first();
                if (volume.equals(Volume.none()) && ! processors.isEmpty()) {
                    for (Volume v: processors.keySet()) {
                        message.first().addResponse(v, null);
                    }
                } else {
                    message.first().addResponse(volume, null);
                }
            }
            // we assume that between iterating over the keys and values of processors
            // that the map doesn't change (probably a bad assumption)
            for (Pair<Volume, ShardedRequestMessage<?>> shard: message.second()) {
                Volume volume = shard.first();
                if (volume.equals(Volume.none())) {
                    for (VolumeProcessor processor: processors.values()) {
                        if (!processor.send(new ShardedRequest(message.first(), shard.second()))) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    VolumeProcessor processor = processors.get(volume);
                    if (processor == null) {
                        processor = new VolumeProcessor(volume);
                        if (processors.putIfAbsent(volume, processor) == null) {
                            try {
                                Futures.addCallback(lookup(volume), processor, SAME_THREAD_EXECUTOR);
                            } catch (Exception e) {
                                // TODO
                                throw Throwables.propagate(e);
                            }
                        } else {
                            processor = processors.get(volume);
                        }
                    }
                    if (!processor.send(new ShardedRequest(message.first(), shard.second()))) {
                        return false;
                    }
                }
            }
            return true;
        }
        
        @Override
        protected void doRun() throws Exception {
            for (VolumeProcessor processor: processors.values()) {
                processor.run();
            }
        }

        @Override
        protected void doStop() {
            for (VolumeProcessor processor: processors.values()) {
                processor.stop();
            }
        }

        @Override
        protected Logger logger() {
            return logger;
        }

        protected class VolumeProcessor extends ExecutedActor<ShardedRequest> implements FutureCallback<BackendSessionExecutor> {

            protected final Volume volume;
            protected final Queue<ShardedRequest> mailbox;
            // not thread-safe
            protected final Set<ListenableFuture<?>> futures;
            protected volatile BackendSessionExecutor backend;
            
            public VolumeProcessor(
                    Volume volume) {
                this.volume = volume;
                this.mailbox = Queues.newConcurrentLinkedQueue();
                this.futures = Collections.newSetFromMap(
                        new WeakHashMap<ListenableFuture<?>, Boolean>());
            }

            @Override
            public void onSuccess(BackendSessionExecutor result) {
                this.backend = result;
                run();
            }

            @Override
            public void onFailure(Throwable t) {
                // FIXME
                throw new AssertionError(t);
            }

            @Override
            protected boolean schedule() {
                if (backend == null) {
                    return false;
                } else {
                    return super.schedule();
                }
            }
            
            @Override
            protected void doRun() throws Exception {
                if (backend != null) {
                    super.doRun();
                }
            }

            @Override
            protected boolean apply(ShardedRequest input) throws Exception {
                BackendSessionExecutor backend = this.backend;
                if (backend == null) {
                    return false;
                }
                ListenableFuture<ShardedResponseMessage<?>> future = backend.submit(
                            Pair.create(MessageSessionRequest.of(session().id(), input.second()), input.first()));
                input.first().addResponse(volume, future);
                return true;
            }

            @Override
            protected Executor executor() {
                return FrontendSessionExecutor.this.executor();
            }

            @Override
            protected Queue<ShardedRequest> mailbox() {
                return mailbox;
            }

            @Override
            protected Logger logger() {
                return logger;
            }   
        }
    }
}