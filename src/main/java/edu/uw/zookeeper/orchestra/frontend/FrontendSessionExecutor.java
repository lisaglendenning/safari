package edu.uw.zookeeper.orchestra.frontend;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutorActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.peer.PeerConnection.ClientPeerConnection;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Ping;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.IErrorResponse;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.PingProcessor;

public class FrontendSessionExecutor extends ExecutorActor<OperationFuture<?>> implements TaskExecutor<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> {
    
    protected final Logger logger;
    protected final Executor executor;
    protected final LinkedQueue<OperationFuture<?>> mailbox;
    protected final Publisher publisher;
    protected final Session session;
    protected final Lookups<ZNodeLabel.Path, Volume> volumes;
    protected final Lookups<Identifier, Identifier> assignments;
    protected final BackendLookups backends;
    protected final Processors.UncheckedProcessor<Pair<SessionOperation.Request<Records.Request>, Records.Response>, Message.ServerResponse<Records.Response>> processor;
    protected volatile LinkedIterator<OperationFuture<?>> finger;

    protected FrontendSessionExecutor(
            Session session,
            Publisher publisher,
            Processors.UncheckedProcessor<Pair<SessionOperation.Request<Records.Request>, Records.Response>, Message.ServerResponse<Records.Response>> processor,
            CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
            CachedFunction<Identifier, Identifier> assignmentLookup,
            Function<Identifier, Identifier> ensembleForPeer,
            CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup,
            Executor executor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.executor = executor;
        this.mailbox = LinkedQueue.create();
        this.publisher = publisher;
        this.session = session;
        this.processor = processor;
        this.volumes = new Lookups<ZNodeLabel.Path, Volume>(
                volumeLookup, 
                this,
                MoreExecutors.sameThreadExecutor());
        this.assignments = new Lookups<Identifier, Identifier>(
                assignmentLookup, 
                this,
                MoreExecutors.sameThreadExecutor());
        this.backends = new BackendLookups(ensembleForPeer, connectionLookup);
        this.finger = null;
    }
    
    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected LinkedQueue<OperationFuture<?>> mailbox() {
        return mailbox;
    }

    public Session session() {
        return session;
    }
    
    protected Lookups<ZNodeLabel.Path, Volume> volumes() {
        return volumes;
    }
    
    protected Lookups<Identifier, Identifier> assignments() {
        return assignments;
    }
    
    protected BackendLookups backends() {
        return backends;
    }
    
    protected Processors.UncheckedProcessor<Pair<SessionOperation.Request<Records.Request>, Records.Response>, Message.ServerResponse<Records.Response>> processor() {
        return processor;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
            Message.ClientRequest<Records.Request> request) {
        OperationFuture<Message.ServerResponse<Records.Response>> task = newRequest(request,SettableFuturePromise.<Message.ServerResponse<Records.Response>>create());
        send(task);
        return task;
    }
    
    @Override
    public void send(OperationFuture<?> message) {
        // short circuit pings
        if ((message instanceof RequestTask) && (((RequestTask) message).task().getXid() == OpCodeXid.PING.getXid())) {
            try {
                while (message.call() != OperationFuture.State.PUBLISHED) {}
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Submitting {}", message);
            }
            super.send(message);
            message.addListener(this, MoreExecutors.sameThreadExecutor());
        }
    }

    public OperationFuture<Message.ServerResponse<Records.Response>> newRequest(
            Message.ClientRequest<Records.Request> request, Promise<Message.ServerResponse<Records.Response>> promise) {
        promise = LoggingPromise.create(logger, promise);
        if (request.getXid() == OpCodeXid.PING.getXid()) {
            return new LocalRequestTask(OperationFuture.State.SUBMITTING, request, promise);
        } else {
            return new BackendRequestTask(OperationFuture.State.WAITING, request, promise);
        }
    }
    
    public void handleTransition(Identifier peer, Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            // FIXME
            throw new UnsupportedOperationException();
        }
    }
    
    public void handleResponse(Identifier peer, ShardedResponseMessage<?> message) {
        if (state.get() == State.TERMINATED) {
            // FIXME
            throw new IllegalStateException();
        }
        Identifier ensemble = backends().getEnsembleForPeer().apply(peer);
        backends().get().get(ensemble).handleResponse(message);
        schedule();
    }

    @Override
    protected void doRun() throws Exception {
        OperationFuture<?> next;
        while ((next = finger.peekNext()) != null) {
            if (! apply(next)) {
                break;
            }
        }
    }
    
    @Override
    protected boolean apply(OperationFuture<?> task) throws Exception {
        if (state() != State.TERMINATED) {
            while (true) {
                OperationFuture.State state = task.state();
                if (OperationFuture.State.PUBLISHED == state) {
                    finger.next();
                    finger.remove();
                    break;
                }
                else if ((OperationFuture.State.WAITING == state)
                        && ((finger.peekPrevious() != null) && (finger.peekPrevious().state().compareTo(OperationFuture.State.SUBMITTING) < 0))) {
                    // we need to preserve ordering of requests per volume
                    // as a proxy for this requirement, 
                    // don't submit until the task before us has submitted
                    // (note this is stronger than necessary)
                    break;
                } else if (task.call() == state) {
                    break;
                }
            }
        }
        return (state() != State.TERMINATED);
    }

    protected static class ListenableFuturePending<V> extends Pair<ListenableFuture<V>, Set<BackendRequestTask>> {
    
        public ListenableFuturePending(
                ListenableFuture<V> first) {
            super(first, Collections.synchronizedSet(Sets.<BackendRequestTask>newHashSet()));
        }
        
    }

    protected static class Lookups<I,O> extends Pair<CachedFunction<I,O>, ConcurrentMap<I, ListenableFuturePending<O>>> {
    
        protected final Runnable runnable;
        protected final Executor executor;
        
        public Lookups(
                CachedFunction<I, O> first,
                Runnable runnable,
                Executor executor) {
            super(first, new MapMaker().<I, ListenableFuturePending<O>>makeMap());
            this.runnable = runnable;
            this.executor = executor;
        }
        
        public Optional<O> apply(I input, BackendRequestTask listener) throws Exception {
            O output = first().first().apply(input);
            if (output != null) {
                return Optional.of(output);
            } else {
                ListenableFuturePending<O> pending = second().get(input);
                if (pending == null) {
                    ListenableFuture<O> future = first().apply(input);
                    pending = new ListenableFuturePending<O>(future);
                    ListenableFuturePending<O> prev = second().putIfAbsent(input, pending);
                    if (prev != null) {
                        future.cancel(true);
                        pending = prev;
                    } else {
                        pending.first().addListener(runnable, executor);
                    }
                }
                listener.pending.add(pending);
                pending.second().add(listener);
                // TODO: if pending was processed...
                return Optional.absent();
            }
        }
    }

    // TODO handle disconnects and reconnects
    protected class BackendLookups extends Lookups<Identifier, BackendSessionExecutor> implements Reference<ConcurrentMap<Identifier, BackendSessionExecutor>> {
    
        protected final ConcurrentMap<Identifier, BackendSessionExecutor> backendSessions;
        protected final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup;
        protected final Function<Identifier, Identifier> ensembleForPeer;
        
        public BackendLookups(
                Function<Identifier, Identifier> ensembleForPeer,
                CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup) {
            this(ensembleForPeer, connectionLookup, 
                    new MapMaker().<Identifier, BackendSessionExecutor>makeMap());
        }
        
        public BackendLookups(
                final Function<Identifier, Identifier> ensembleForPeer,
                final CachedFunction<Identifier, ClientPeerConnection<Connection<? super MessagePacket>>> connectionLookup,
                final ConcurrentMap<Identifier, BackendSessionExecutor> backendSessions) {
            super(CachedFunction.create(
                    new Function<Identifier, BackendSessionExecutor>() {
                        @Override
                        public BackendSessionExecutor apply(Identifier ensemble) {
                            return backendSessions.get(ensemble);
                        }
                    }, 
                    new AsyncFunction<Identifier, BackendSessionExecutor>() {
                        @Override
                        public ListenableFuture<BackendSessionExecutor> apply(Identifier ensemble) throws Exception {
                            final EstablishBackendSessionTask task = new EstablishBackendSessionTask(
                                    session(),
                                    Optional.<Session>absent(),
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
                                                    self.session().id(), 
                                                    task.task(),
                                                    Futures.getUnchecked(task),
                                                    Futures.getUnchecked(task.connection()),
                                                    self.publisher,
                                                    MoreExecutors.sameThreadExecutor());
                                            BackendSessionExecutor prev = backendSessions.putIfAbsent(backend.getEnsemble(), backend);
                                            if (prev != null) {
                                                // TODO
                                                throw new AssertionError();
                                            }
                                            return backend;
                                        }
                                        
                                    });
                        }
                    }),
                    FrontendSessionExecutor.this,
                    MoreExecutors.sameThreadExecutor());
            this.ensembleForPeer = ensembleForPeer;
            this.connectionLookup = connectionLookup;
            this.backendSessions = backendSessions;
        }
        
        public Function<Identifier, Identifier> getEnsembleForPeer() {
            return ensembleForPeer;
        }
        
        @Override
        public ConcurrentMap<Identifier, BackendSessionExecutor> get() {
            return backendSessions;
        }
    }

    protected abstract class RequestTask extends PromiseTask<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> implements OperationFuture<Message.ServerResponse<Records.Response>>, Runnable {

        protected final AtomicReference<State> state;

        public RequestTask(
                State state,
                Message.ClientRequest<Records.Request> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
            this.state = new AtomicReference<State>(state);
        }
        
        @Override
        public int getXid() {
            return task().getXid();
        }
        
        @Override
        public boolean set(Message.ServerResponse<Records.Response> result) {
            if ((result.getRecord().getOpcode() != task().getRecord().getOpcode())
                    && (! (result.getRecord() instanceof Operation.Error))) {
                throw new IllegalArgumentException(result.toString());
            }
            
            OperationFuture.State state = state();
            if (OperationFuture.State.COMPLETE.compareTo(state) > 0) {
                this.state.compareAndSet(state, OperationFuture.State.COMPLETE);
            }

            return super.set(result);
        }
        
        @Override
        public OperationFuture.State state() {
            return state.get();
        }

        @Override
        public void run() {
            FrontendSessionExecutor.this.run();
        }

        @Override
        public State call() throws Exception {
            if (logger.isTraceEnabled()) {
                logger.trace("{}", this);
            }
            return state();
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("task", task())
                    .add("future", delegate())
                    .add("state", state())
                    .toString();
        }

        protected ListenableFuture<Message.ServerResponse<Records.Response>> complete() throws InterruptedException, ExecutionException {
            if (state.get() == OperationFuture.State.SUBMITTING) {
                Records.Response result = null;
                switch (task().getRecord().getOpcode()) {
                case CLOSE_SESSION:
                {
                    result = Records.newInstance(IDisconnectResponse.class);
                    break;
                }
                case PING:
                {
                    result = PingProcessor.getInstance().apply((Ping.Request) task().getRecord());
                    break;
                }
                default:
                    break;
                }
                if (result != null) {
                    complete(result);
                }
            }
            return this;
        }
        
        protected boolean complete(Records.Response result) {
            if (isDone()) {
                return false;
            }
            SessionOperation.Request<Records.Request> request =
                    SessionRequest.of(session().id(), task(), task());
            Message.ServerResponse<Records.Response> message = processor().apply(
                    Pair.create(request, result));
            return set(message);
        }

        protected boolean publish() {
            if (this.state.compareAndSet(OperationFuture.State.COMPLETE, OperationFuture.State.PUBLISHED)) {
                // TODO: exception?
                publisher.post(Futures.getUnchecked(this));
                return true;
            } else {
                return false;
            }
        }
    }
    
    protected class LocalRequestTask extends RequestTask {

        public LocalRequestTask(
                State state,
                Message.ClientRequest<Records.Request> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(state, task, delegate);
        }
        
        @Override
        public State call() throws Exception {
            State state = super.call();
            switch (state) {
            case WAITING:
                this.state.compareAndSet(state, OperationFuture.State.SUBMITTING);
                break;
            case SUBMITTING:
                complete();
                break;
            case COMPLETE:
                publish();
                break;
            default:
                break;
            }
            
            return state();
        }
    }

    protected class BackendRequestTask extends RequestTask {

        protected final ImmutableSet<ZNodeLabel.Path> paths;
        protected final Map<ZNodeLabel.Path, Volume> volumes;
        protected final BiMap<Volume, ShardedRequestMessage<?>> shards;
        protected final Map<Identifier, BackendSessionExecutor.BackendResponseTask> submitted;
        protected final Set<ListenableFuturePending<?>> pending;

        public BackendRequestTask(
                State state,
                Message.ClientRequest<Records.Request> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(state, task, delegate);
            this.paths = ImmutableSet.copyOf(PathsOfRequest.getPathsOfRequest(task.getRecord()));
            this.volumes = Collections.synchronizedMap(Maps.<ZNodeLabel.Path, Volume>newHashMap());
            this.shards = Maps.synchronizedBiMap(HashBiMap.<Volume, ShardedRequestMessage<?>>create());
            this.submitted = Collections.synchronizedMap(Maps.<Identifier, BackendSessionExecutor.BackendResponseTask>newHashMap());
            this.pending = Collections.synchronizedSet(Sets.<ListenableFuturePending<?>>newHashSet());
        }
        
        @Override
        public int getXid() {
            return task().getXid();
        }

        public ImmutableSet<ZNodeLabel.Path> paths() {
            return paths;
        }

        @Override
        public State call() throws Exception {
            switch (super.call()) {
            case WAITING:
            {
                Optional<Map<ZNodeLabel.Path, Volume>> volumes = getVolumes(paths());
                if (! volumes.isPresent()) {
                    break;
                }
                Set<Volume> uniqueVolumes = getUniqueVolumes(volumes.get().values());
                Optional<Map<Volume, Set<BackendSessionExecutor>>> backends = getConnections(uniqueVolumes);
                if (! backends.isPresent()) {
                    break;
                }
                BiMap<Volume, ShardedRequestMessage<?>> shards;
                try {
                    shards = getShards(volumes.get());
                } catch (KeeperException e) {
                    // fail
                    complete(new IErrorResponse(e.code()));
                    break;
                }
                submit(shards, backends.get());
                break;
            }
            case SUBMITTING:
            {
                complete();
                break;
            }
            case COMPLETE:
            {
                publish();
                break;
            }
            default:
                break;
            }
            
            return state();
        }

        protected Optional<Map<ZNodeLabel.Path, Volume>> getVolumes(Set<ZNodeLabel.Path> paths) throws Exception {
            Sets.SetView<ZNodeLabel.Path> difference = Sets.difference(paths, volumes.keySet());
            for (ZNodeLabel.Path path: difference) {
                Optional<Volume> v = volumes().apply(path, this);
                if (v.isPresent()) {
                    volumes.put(path, v.get());
                }
            }
            if (difference.isEmpty()) {
                return Optional.of(volumes);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("Waiting for path lookups {}", difference);
                }
                return Optional.<Map<ZNodeLabel.Path, Volume>>absent();
            }
        }
        
        protected ImmutableSet<Volume> getUniqueVolumes(Collection<Volume> volumes) {
            return volumes.isEmpty()
                        ? ImmutableSet.of(Volume.none())
                        : ImmutableSet.copyOf(volumes);
        }
        
        protected BiMap<Volume, ShardedRequestMessage<?>> getShards(Map<ZNodeLabel.Path, Volume> volumes) throws KeeperException {
            ImmutableSet<Volume> uniqueVolumes = getUniqueVolumes(volumes.values());
            Sets.SetView<Volume> difference = Sets.difference(uniqueVolumes, shards.keySet());
            if (! difference.isEmpty()) {
                if ((OpCode.MULTI == task().getRecord().getOpcode())
                        && ! ImmutableSet.of(Volume.none()).equals(uniqueVolumes)) {
                    Map<Volume, List<Records.MultiOpRequest>> byShardOps = Maps.newHashMapWithExpectedSize(difference.size());
                    for (Records.MultiOpRequest op: (IMultiRequest) task().getRecord()) {
                        ZNodeLabel.Path[] paths = PathsOfRequest.getPathsOfRequest(op);
                        assert (paths.length > 0);
                        for (ZNodeLabel.Path path: paths) {
                            Volume v = volumes.get(path);
                            if (! difference.contains(v)) {
                                continue;
                            }
                            List<Records.MultiOpRequest> ops;
                            if (byShardOps.containsKey(v)) {
                                ops = byShardOps.get(v);
                            } else {
                                ops = Lists.newLinkedList();
                                byShardOps.put(v, ops);
                            }
                            ops.add(validate(v, op));
                        }
                    }
                    for (Map.Entry<Volume, List<Records.MultiOpRequest>> e: byShardOps.entrySet()) {
                        shards.put(e.getKey(), 
                                ShardedRequestMessage.of(
                                        e.getKey().getId(),
                                        ProtocolRequestMessage.of(
                                                task().getXid(),
                                                new IMultiRequest(e.getValue()))));
                    }
                } else {
                    for (Volume v: difference) {
                        validate(v, task().getRecord());
                        shards.put(v, ShardedRequestMessage.of(v.getId(), task()));
                    }
                }
            }
            assert (difference.isEmpty());
            return shards;
        }
        
        protected <T extends Records.Request> T validate(Volume volume, T request) throws KeeperException {
            switch (request.getOpcode()) {
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

        protected Optional<Map<Volume, Set<BackendSessionExecutor>>> getConnections(Set<Volume> volumes) throws Exception {
            Map<Volume, Set<BackendSessionExecutor>> backends = Maps.newHashMapWithExpectedSize(volumes.size());
            for (Volume v: volumes) {
                if (v.equals(Volume.none())) {
                    boolean done = true;
                    Set<BackendSessionExecutor> all = Sets.newHashSetWithExpectedSize(backends().get().size());
                    for (Identifier ensemble: backends().get().keySet()) {
                        Optional<BackendSessionExecutor> backend = backends().apply(ensemble, this);
                        if (backend.isPresent()) {
                            all.add(backend.get());
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Waiting for backend {}", ensemble);
                            }
                            done = false;
                        }
                    }
                    if (done) {
                        backends.put(v, all);
                    }
                } else {
                    Optional<Identifier> assignment = assignments().apply(v.getId(), this);
                    if (assignment.isPresent()) {
                        Optional<BackendSessionExecutor> backend = backends().apply(assignment.get(), this);
                        if (backend.isPresent()) {
                            backends.put(v, ImmutableSet.of(backend.get()));
                        }
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Waiting for assignment {}", v.getId());
                        }
                    }
                }
            }
            Sets.SetView<Volume> difference = Sets.difference(volumes, backends.keySet());
            if (difference.isEmpty()) {
                return Optional.of(backends);
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("Waiting for volume connections {}", difference);
                }
                return Optional.absent();
            }
        }
        
        protected Map<Identifier, BackendSessionExecutor.BackendResponseTask> submit(
                Map<Volume, ShardedRequestMessage<?>> shards,
                Map<Volume, Set<BackendSessionExecutor>> backends) throws Exception {
            this.state.compareAndSet(OperationFuture.State.WAITING, OperationFuture.State.SUBMITTING);
            for (Map.Entry<Volume, ShardedRequestMessage<?>> e: shards.entrySet()) {
                for (BackendSessionExecutor backend: backends.get(e.getKey())) {
                    ClientPeerConnection<?> connection = backend.getConnection();
                    Identifier k = connection.remoteAddress().getIdentifier();
                    if (! submitted.containsKey(k)) {
                        BackendSessionExecutor.BackendResponseTask task = backend.submit(
                                Pair.<OperationFuture<?>, ShardedRequestMessage<?>>create(this, e.getValue()));
                        submitted.put(k, task);
                    }
                }
            }
            return submitted;
        }
        
        @Override
        protected ListenableFuture<Message.ServerResponse<Records.Response>> complete() throws InterruptedException, ExecutionException {
            if (state.get() != OperationFuture.State.SUBMITTING) {
                return this;
            }
            
            if (submitted.isEmpty()) {
                return super.complete();
            }
            
            Records.Response result = null;
            if (OpCode.MULTI == task().getRecord().getOpcode()) {
                Map<Volume, ListIterator<Records.MultiOpResponse>> responses = Maps.newHashMapWithExpectedSize(shards.size());
                for (BackendSessionExecutor.BackendResponseTask task: submitted.values()) {
                    if (! task.isDone()) {
                        break;
                    } else {
                        responses.put(
                                shards.inverse().get(task.task()), 
                                ((IMultiResponse) task.get().getRecord()).listIterator());
                    }
                }
                if (Sets.difference(shards.keySet(), responses.keySet()).isEmpty()) {
                    Map<Volume, ListIterator<Records.MultiOpRequest>> requests = Maps.newHashMapWithExpectedSize(shards.size());
                    for (Map.Entry<Volume, ShardedRequestMessage<?>> e: shards.entrySet()) {
                        requests.put(
                                e.getKey(), 
                                ((IMultiRequest) e.getValue().getRecord()).listIterator());
                    }
                    IMultiRequest multi = (IMultiRequest) task().getRecord();
                    List<Records.MultiOpResponse> ops = Lists.newArrayListWithCapacity(multi.size());
                    for (Records.MultiOpRequest op: multi) {
                        Volume v = null;
                        Records.MultiOpResponse response = null;
                        for (Map.Entry<Volume, ListIterator<Records.MultiOpRequest>> e: requests.entrySet()) {
                            if (! e.getValue().hasNext()) {
                                continue;
                            }
                            if (! op.equals(e.getValue().next())) {
                                e.getValue().previous();
                                continue;
                            }
                            if ((response == null) 
                                    || (v.getDescriptor().getRoot().prefixOf(
                                            e.getKey().getDescriptor().getRoot()))) {
                                v = e.getKey();
                                response = responses.get(v).next();
                            }
                        }
                        assert (response != null);
                        ops.add(response);
                    }
                    result = new IMultiResponse(ops);
                }
            } else {
                BackendSessionExecutor.BackendResponseTask selected = null;
                for (BackendSessionExecutor.BackendResponseTask task: submitted.values()) {
                    if (! task.isDone()) {
                        selected = null;
                        break;
                    } else {
                         if (selected == null) {
                            selected = task;
                        } else {
                            if ((selected.get().getRecord() instanceof Operation.Error) || (task.get().getRecord() instanceof Operation.Error)) {
                                if (task().getRecord().getOpcode() != OpCode.CLOSE_SESSION) {
                                    throw new UnsupportedOperationException();
                                }
                            } else {
                                // we should only get here for create, create2, and delete
                                // and only if the path is for a volume root
                                // in that case, pick the response that came from the volume root
                                if (shards.inverse().get(selected.task()).getDescriptor().getRoot().prefixOf(
                                        shards.inverse().get(task.task()).getDescriptor().getRoot())) {
                                    selected = task;
                                }
                            }
                        }
                    }
                }
                if (selected != null) {
                    result = selected.get().getRecord();
                }
            }
            if (result != null) {
                super.complete(result);
            }
            return this;
        }
        
        @Override
        protected boolean publish() {
            boolean published = super.publish();
            if (published) {
                for (Identifier peer: submitted.keySet()) {
                    Identifier ensemble = backends().getEnsembleForPeer().apply(peer);
                    BackendSessionExecutor backend = backends().get().get(ensemble);
                    backend.run();
                }
            }
            return published;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("task", task())
                    .add("future", delegate())
                    .add("state", state())
                    .add("volumes", volumes)
                    .add("shards", shards)
                    .add("submitted", submitted)
                    .toString();
        }
    }
}