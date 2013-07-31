package edu.uw.zookeeper.orchestra.frontend;

import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.orchestra.CachedFunction;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.orchestra.peer.protocol.MessagePacket;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.orchestra.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.orchestra.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TaskExecutor;

// FIXME: disconnect
public class FrontendSessionExecutor extends AbstractActor<FrontendSessionExecutor.RequestTask> implements TaskExecutor<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> {
    
    protected final Session session;
    protected final Publisher publisher;
    protected final Lookups<ZNodeLabel.Path, Volume> volumes;
    protected final Lookups<Identifier, Identifier> assignments;
    protected final BackendLookups backends;
    protected final Queue<ShardedResponseMessage<?>> responses;

    protected FrontendSessionExecutor(
            Session session,
            Publisher publisher,
            CachedFunction<ZNodeLabel.Path, Volume> volumeLookup,
            CachedFunction<Identifier, Identifier> assignmentLookup,
            CachedFunction<Identifier, ? extends Connection<MessagePacket>> connectionLookup,
            Executor executor) {
        super(executor, FutureQueue.<RequestTask>create(), AbstractActor.newState());
        this.session = session;
        this.publisher = publisher;
        this.volumes = new Lookups<ZNodeLabel.Path, Volume>(volumeLookup);
        this.assignments = new Lookups<Identifier, Identifier>(assignmentLookup);
        this.backends = new BackendLookups(connectionLookup);
        this.responses = new ConcurrentLinkedQueue<ShardedResponseMessage<?>>();
    }
    
    protected static class Lookups<I,O> extends Pair<CachedFunction<I,O>, ConcurrentMap<I, ListenableFuturePending<O>>> {

        public Lookups(CachedFunction<I, O> first) {
            super(first, new MapMaker().<I, ListenableFuturePending<O>>makeMap());
        }
        
        public Optional<O> apply(I input, RequestTask listener) throws Exception {
            O output = first().first().apply(input);
            if (output != null) {
                return Optional.of(output);
            } else {
                ListenableFuturePending<O> pending = second().get(input);
                if (pending == null) {
                    ListenableFuture<O> future = first().apply(input);
                    ListenableFuturePending<O> prev = second().putIfAbsent(input, new ListenableFuturePending<O>(future));
                    if (prev != null) {
                        future.cancel(true);
                        pending = prev;
                    }
                    // TODO add runnable
                }
                listener.pending.add(pending);
                pending.second().add(listener);
                // TODO: if pending was processed...
                return Optional.absent();
            }
        }
    }
    
    // TODO handle disconnects and reconnects
    protected class BackendLookups extends Lookups<Identifier, BackendSessionTask> implements Reference<ConcurrentMap<Identifier, BackendSessionTask>> {

        protected final ConcurrentMap<Identifier, BackendSessionTask> backendSessions;
        protected final CachedFunction<Identifier, ? extends Connection<MessagePacket>> connectionLookup;
        
        public BackendLookups(
                CachedFunction<Identifier, ? extends Connection<MessagePacket>> connectionLookup) {
            this(connectionLookup, new MapMaker().<Identifier, BackendSessionTask>makeMap());
        }
        
        public ConcurrentMap<Identifier, BackendSessionTask> get() {
            return backendSessions;
        }
        
        public BackendLookups(
                final CachedFunction<Identifier, ? extends Connection<MessagePacket>> connectionLookup,
                final ConcurrentMap<Identifier, BackendSessionTask> backendSessions) {
            super(CachedFunction.create(
                    new Function<Identifier, BackendSessionTask>() {
                        @Override
                        public BackendSessionTask apply(Identifier ensemble) {
                            return backendSessions.get(ensemble);
                        }
                    }, 
                    new AsyncFunction<Identifier, BackendSessionTask>() {
                        @Override
                        public ListenableFuture<BackendSessionTask> apply(Identifier ensemble) throws Exception {
                            final BackendSessionTask backend = new BackendSessionTask(
                                    Optional.<Session>absent(),
                                    ensemble, 
                                    connectionLookup.apply(ensemble),
                                    SettableFuturePromise.<Session>create());
                            return Futures.transform(
                                    backend, 
                                    new Function<Session, BackendSessionTask>() {
                                        @Override
                                        @Nullable
                                        public BackendSessionTask apply(Session input) {
                                            BackendSessionTask prev = backendSessions.putIfAbsent(backend.task(), backend);
                                            if (prev != null) {
                                                // TODO
                                                throw new AssertionError();
                                            }
                                            return backend;
                                        }
                                        
                                    });
                        }
                    }));
            this.connectionLookup = connectionLookup;
            this.backendSessions = backendSessions;
        }
    }
    
    protected static class ListenableFuturePending<V> extends Pair<ListenableFuture<V>, Set<RequestTask>> {

        public ListenableFuturePending(
                ListenableFuture<V> first) {
            super(first, Collections.synchronizedSet(Sets.<RequestTask>newHashSet()));
        }
        
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

    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
            Message.ClientRequest<Records.Request> request) {
        Promise<Message.ServerResponse<Records.Response>> promise = SettableFuturePromise.create();
        RequestTask task = new RequestTask(request, promise);
        send(task);
        return task;
    }
    
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            // FIXME
        }
    }
    
    public void handleResponse(ShardedResponseMessage<?> message) {
        if (state.get() == State.TERMINATED) {
            throw new IllegalStateException();
        }
        responses.add(message);
        schedule();
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
        RequestTask prev = null;
        for (RequestTask task: mailbox) {
            task.apply(prev);
            prev = task;
        }
        
        ShardedResponseMessage<?> response = null;
        while ((response = responses.poll()) != null) {
            int xid = response.getXid();
            if (OpCodeXid.has(xid)) {
                publisher.post(response.getResponse());
            } else {
                prev = null;
                for (RequestTask request: mailbox) {
                    if (request.handleResponse(response)) {
                        request.apply(prev);
                        break;
                    }
                    prev = request;
                }
                
                super.doRun();
            }
        }
        
        super.doRun();
    }
    
    @Override
    protected boolean apply(RequestTask input) throws Exception {
        boolean running = super.apply(input);
        input.publish();
        return running;
    }

    protected class BackendSessionTask extends PromiseTask<Identifier, Session> implements Runnable, FutureCallback<MessagePacket> {
        
        protected final Optional<Session> existing;
        protected final ListenableFuture<? extends Connection<MessagePacket>> connection;
        
        public BackendSessionTask(
                Optional<Session> existing,
                Identifier ensemble,
                ListenableFuture<? extends Connection<MessagePacket>> connection,
                Promise<Session> promise) throws Exception {
            super(ensemble, promise);
            this.existing = existing;
            this.connection = connection;
            this.connection.addListener(this, MoreExecutors.sameThreadExecutor());
        }
        
        public ListenableFuture<? extends Connection<MessagePacket>> connection() {
            return connection;
        }
        
        @Subscribe
        public void handleTransition(Automaton.Transition<?> event) {
            if (event.to() == Connection.State.CONNECTION_CLOSED) {
                Futures.getUnchecked(this.connection).unregister(this);
                if (! isDone()) {
                    setException(new ClosedChannelException());
                }
            }
        }
        
        @Subscribe
        public void handleMessage(MessagePacket message) {
            switch (message.first().type()) {
            case MESSAGE_TYPE_SESSION_OPEN_RESPONSE:
            {
                MessageSessionOpenResponse response = message.getBody(MessageSessionOpenResponse.class);
                if (response.getSessionId() != session().id()) {
                    break;
                }
                Futures.getUnchecked(this.connection).unregister(this);
                if (! isDone()) {
                    if (response.delegate() instanceof ConnectMessage.Response.Valid) {
                        set(response.delegate().toSession());
                    } else {
                        setException(new KeeperException.SessionExpiredException());
                    }
                }
                break;
            }
            default:
                break;
            }
        }
        
        @Override
        public void run() {
            if (connection.isDone()) {
                Connection<MessagePacket> c;
                try {
                    c = connection.get();
                } catch (Exception e) {
                    if (! isDone()) {
                        setException(e);
                    }
                    return;
                }
                
                ConnectMessage.Request request;
                if (existing.isPresent()) {
                    request = ConnectMessage.Request.RenewRequest.newInstance(
                                existing.get(), 0L);
                } else {
                    request = ConnectMessage.Request.NewRequest.newInstance(
                            session().parameters().timeOut(), 0L);
                }
                
                c.register(this);
                Futures.addCallback(
                        c.write(MessagePacket.of(
                                    MessageSessionOpenRequest.of(
                                        session().id(), request))),
                        this);
            }
        }

        @Override
        public void onSuccess(MessagePacket result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
    
    protected static enum RequestState {
        NEW, LOOKING, SUBMITTING, COMPLETE, PUBLISHED;
    }

    protected class RequestTask extends PromiseTask<Message.ClientRequest<Records.Request>, Message.ServerResponse<Records.Response>> implements Stateful<FrontendSessionExecutor.RequestState>, Runnable {

        protected final AtomicReference<FrontendSessionExecutor.RequestState> state;
        protected final ImmutableSet<ZNodeLabel.Path> paths;
        protected final Map<ZNodeLabel.Path, Volume> volumes;
        protected final Map<Volume, ShardedRequestMessage<?>> shards;
        protected final Map<Identifier, ResponseTask> submitted;
        protected volatile ListenableFuture<List<ShardedResponseMessage<Records.Response>>> submittedFuture;
        protected final Set<ListenableFuturePending<?>> pending;
        
        public RequestTask(
                Message.ClientRequest<Records.Request> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
            this.state = new AtomicReference<FrontendSessionExecutor.RequestState>(RequestState.NEW);
            this.paths = ImmutableSet.copyOf(PathsOfRequest.getPathsOfRequest(task.getRecord()));
            this.volumes = Collections.synchronizedMap(Maps.<ZNodeLabel.Path, Volume>newHashMap());
            this.shards = Collections.synchronizedMap(Maps.<Volume, ShardedRequestMessage<?>>newHashMap());
            this.submitted = Collections.synchronizedMap(Maps.<Identifier, ResponseTask>newHashMap());
            this.submittedFuture = null;
            this.pending = Collections.synchronizedSet(Sets.<ListenableFuturePending<?>>newHashSet());
        }
        
        @Override
        public FrontendSessionExecutor.RequestState state() {
            return state.get();
        }
        
        public ImmutableSet<ZNodeLabel.Path> paths() {
            return paths;
        }

        @Override
        public void run() {
            FrontendSessionExecutor.this.run();
        }
        
        public boolean handleResponse(ShardedResponseMessage<?> response) {
            if (task().getXid() != response.getXid()) {
                return false;
            }
            FrontendSessionExecutor.ResponseTask task = submitted.get(response.getIdentifier());
            if (task == null) {
                // FIXME
                throw new AssertionError();
            }
            if (! task.set(response)) {
                // FIXME
                throw new AssertionError();
            }
            return true;
        }
        
        public ListenableFuture<Message.ServerResponse<Records.Response>> apply(RequestTask prev) throws Exception {
            switch (state()) {
            case NEW:
            case LOOKING:
            {
                Optional<Map<ZNodeLabel.Path, Volume>> volumes = getVolumes(paths());
                if (! volumes.isPresent()) {
                    break;
                }
                Set<Volume> uniqueVolumes = getUniqueVolumes(volumes.get().values());
                Optional<Map<Volume, BackendSessionTask>> backends = getConnections(uniqueVolumes);
                if (! backends.isPresent()) {
                    break;
                }
                if ((prev != null) && (prev.state().compareTo(RequestState.SUBMITTING) < 0)) {
                    // we need to preserve ordering of requests per volume
                    // as a proxy for this requirement, 
                    // don't submit until the task before us has submitted
                    // (note this is stronger than necessary)
                    break;
                }
                submit(getShards(volumes.get()), backends.get());
            }
            case SUBMITTING:
            {
                complete();
            }
            default:
                break;
            }
            
            return this;
        }

        protected Optional<Map<ZNodeLabel.Path, Volume>> getVolumes(Set<ZNodeLabel.Path> paths) throws Exception {
            this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
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
                return Optional.<Map<ZNodeLabel.Path, Volume>>absent();
            }
        }
        
        protected ImmutableSet<Volume> getUniqueVolumes(Collection<Volume> volumes) {
            return volumes.isEmpty()
                        ? ImmutableSet.of(Volume.none())
                        : ImmutableSet.copyOf(volumes);
        }
        
        protected Map<Volume, ShardedRequestMessage<?>> getShards(Map<ZNodeLabel.Path, Volume> volumes) throws Exception {
            this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
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
                            ops.add(op);
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
                        shards.put(v, ShardedRequestMessage.of(v.getId(), task()));
                    }
                }
            }
            assert (difference.isEmpty());
            return shards;
        }

        protected Optional<Map<Volume, BackendSessionTask>> getConnections(Set<Volume> volumes) throws Exception {
            this.state.compareAndSet(RequestState.NEW, RequestState.LOOKING);
            Map<Volume, BackendSessionTask> backends = Maps.newHashMapWithExpectedSize(volumes.size());
            for (Volume v: volumes) {
                Optional<Identifier> assignment = assignments().apply(v.getId(), this);
                if (assignment.isPresent()) {
                    Optional<BackendSessionTask> backend = backends().apply(assignment.get(), this);
                    if (backend.isPresent()) {
                        backends.put(v, backend.get());
                    }
                }
            }
            if (Sets.difference(volumes, backends.keySet()).isEmpty()) {
                return Optional.of(backends);
            } else {
                return Optional.absent();
            }
        }
        
        protected Map<Identifier, FrontendSessionExecutor.ResponseTask> submit(
                Map<Volume, ShardedRequestMessage<?>> shards,
                Map<Volume, BackendSessionTask> backends) throws Exception {
            this.state.compareAndSet(RequestState.LOOKING, RequestState.SUBMITTING);
            for (Map.Entry<Volume, ShardedRequestMessage<?>> e: shards.entrySet()) {
                if (submitted.containsKey(e.getKey().getId())) {
                    continue;
                }
                BackendSessionTask backend = backends.get(e.getKey());
                Connection<MessagePacket> connection = backend.connection().get();
                MessagePacket message = MessagePacket.of(
                        MessageSessionRequest.of(session().id(), e.getValue()));
                ListenableFuture<MessagePacket> future = connection.write(message);
                FrontendSessionExecutor.ResponseTask task = new ResponseTask(e.getValue(), future);
                submitted.put(e.getKey().getId(), task);
            }
            // TODO submittedFuture
            return submitted;
        }
        
        @SuppressWarnings("unchecked")
        protected ListenableFuture<Message.ServerResponse<Records.Response>> complete() throws InterruptedException, ExecutionException {
            if (state.get() != RequestState.SUBMITTING) {
                return this;
            }
            // FIXME
            Message.ServerResponse<Records.Response> result = null;
            for (FrontendSessionExecutor.ResponseTask response: submitted.values()) {
                if (! response.isDone()) {
                    result = null;
                    break;
                } else {
                    result = (Message.ServerResponse<Records.Response>) response.get();
                }
            }
            if (result != null) {
                this.state.compareAndSet(RequestState.SUBMITTING, RequestState.COMPLETE);
                set(result);
            }
            return this;
        }
        
        public void publish() throws InterruptedException, ExecutionException {
            if (this.state.compareAndSet(RequestState.COMPLETE, RequestState.PUBLISHED)) {
                publisher.post(get());
            }
        }
    }

    protected static class ResponseTask extends PromiseTask<ShardedRequestMessage<?>, ShardedResponseMessage<?>> implements FutureCallback<MessagePacket> {

        protected final ListenableFuture<MessagePacket> future;

        public ResponseTask(
                ShardedRequestMessage<?> request,
                ListenableFuture<MessagePacket> future) {
            this(request, future, SettableFuturePromise.<ShardedResponseMessage<?>>create());
        }
        
        public ResponseTask(
                ShardedRequestMessage<?> request,
                ListenableFuture<MessagePacket> future,
                Promise<ShardedResponseMessage<?>> delegate) {
            super(request, delegate);
            this.future = future;
            Futures.addCallback(future, this);
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
        public void onSuccess(MessagePacket result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
}