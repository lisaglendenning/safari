package edu.uw.zookeeper.safari.backend;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Queues;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.WaitingActor;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.AbstractConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.client.PendingQueueClientExecutor;
import edu.uw.zookeeper.protocol.client.PendingQueueClientExecutor.PendingTask;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.VersionedValue;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.VolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.volumes.VolumeVersionCache;
import edu.uw.zookeeper.safari.storage.volumes.VolumeVersionCache.CachedVolume;

public final class ShardedClientExecutor<C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> extends PendingQueueClientExecutor<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>, ShardedClientExecutor.ShardedRequestTask, C, ShardedClientExecutor.PendingShardedTask> {

    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> ShardedClientExecutor<C> fromConnect(
            Function<Identifier, CachedVolume> idToVersion,
            AsyncFunction<VersionedId, ? extends VolumeVersion<?>> versionToState,
            AsyncFunction<Identifier, ZNodePath> idToPath,
            ConnectTask<C> connect,
            ScheduledExecutorService scheduler) {
        return create(
                idToVersion,
                versionToState,
                idToPath,
                connect,
                connect.task().second(),
                TimeValue.milliseconds(connect.task().first().getTimeOut()),
                scheduler);
    }

    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> ShardedClientExecutor<C> create(
            Function<Identifier, CachedVolume> idToVersion,
            AsyncFunction<VersionedId, ? extends VolumeVersion<?>> versionToState,
            AsyncFunction<Identifier, ZNodePath> idToPath,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        return new ShardedClientExecutor<C>(
                idToVersion,
                idToPath,
                versionToState,
                LogManager.getLogger(ShardedClientExecutor.class),
                session, 
                connection,
                timeOut,
                scheduler);
    }

    protected final Tasks actor;
    
    protected ShardedClientExecutor(
            Function<Identifier, CachedVolume> idToVersion,
            AsyncFunction<Identifier, ZNodePath> idToPath,
            AsyncFunction<VersionedId, ? extends VolumeVersion<?>> versionToState,
            Logger logger,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        super(logger, session, connection, timeOut, scheduler);
        this.actor = new Tasks(idToVersion, idToPath, versionToState, logger);
    }

    @Override
    public ListenableFuture<ShardedServerResponseMessage<?>> submit(
            ShardedRequestMessage<?> request, Promise<ShardedServerResponseMessage<?>> promise) {
        ShardedRequestTask task =
                LoggingFutureListener.listen(logger(), 
                        new ShardedRequestTask(request, promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return Futures.nonCancellationPropagating(task);
    }
    
    @Override
    public void onSuccess(PendingShardedTask result) {
        actor.onSuccess(result);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void handleConnectionRead(Operation.Response response) {
        logger().debug("Received: {}", response);
        timer.send(response);
        if (response instanceof Message.ServerResponse<?>) {
            Message.ServerResponse<?> message = (Message.ServerResponse<?>) response;
            if (message.xid() == OpCodeXid.NOTIFICATION.xid()) {
                actor.handleNotification((Message.ServerResponse<IWatcherEvent>) message);
            } else {
                pending.handleConnectionRead(response);
            }
        }
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        super.handleAutomatonTransition(transition);
        actor.handleAutomatonTransition(transition);
    }
    
    @Override
    protected Tasks actor() {
        return actor;
    }

    @Override
    protected Logger logger() {
        return actor.logger();
    }

    protected static final class ShardedRequestTask extends AbstractConnectionClientExecutor.RequestTask<ShardedRequestMessage<?>, ShardedServerResponseMessage<?>> {

        // keep a strong reference to our processor during processing
        @SuppressWarnings("rawtypes")
        protected ShardedClientExecutor.Tasks.ShardProcessor processor;
        
        public ShardedRequestTask(
                ShardedRequestMessage<?> task,
                Promise<ShardedServerResponseMessage<?>> promise) {
            super(task, promise);
            assert(task.xid() != OpCodeXid.PING.xid());
        }
    }
    
    public static final class PendingShardedTask extends PendingTask {

        public static PendingShardedTask create(
                ShardedRequestTask sharded,
                Reference<? extends Message.ClientRequest<?>> task,
                Promise<Message.ServerResponse<?>> delegate) {
            return new PendingShardedTask(sharded, task, delegate);
        }

        protected final ShardedRequestTask sharded;

        public PendingShardedTask(
                ShardedRequestTask sharded,
                Reference<? extends Message.ClientRequest<?>> task,
                Promise<Message.ServerResponse<?>> delegate) {
            super(task, delegate);
            this.sharded = sharded;
        }

        public ShardedRequestTask getSharded() {
            return sharded;
        }
    }

    protected static final class SetOnRun<V> extends ToStringListenableFuture<V> implements Runnable {

        public static <V> SetOnRun<V> forValue(V value) {
            return new SetOnRun<V>(value);
        }
        
        private final V value;
        private final Promise<V> promise;

        private SetOnRun(
                V value) {
            this(value, SettableFuturePromise.<V>create());
        }
        
        private SetOnRun(
                V value,
                Promise<V> promise) {
            this.value = value;
            this.promise = promise;
        }

        @Override
        public void run() {
            if (!isDone()) {
                delegate().set(value);
            }
        }

        @Override
        protected Promise<V> delegate() {
            return promise;
        }

        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
            return super.toStringHelper(helper.addValue(value));
        }
    }
    
    protected final class Tasks extends WaitingActor<ListenableFuture<?>,ShardedRequestTask> implements FutureCallback<PendingShardedTask>, SessionListener {

        private final Function<Identifier, CachedVolume> idToVersion;
        private final AsyncFunction<Identifier, ZNodePath> idToPath;
        private final AsyncFunction<VersionedId, ? extends VolumeVersion<?>> versionToState;
        private final ConcurrentMap<Identifier, ShardProcessor> processors;
        
        protected Tasks(
                Function<Identifier, CachedVolume> idToVersion,
                AsyncFunction<Identifier, ZNodePath> idToPath,
                AsyncFunction<VersionedId, ? extends VolumeVersion<?>> versionToState,
                Logger logger) {
            super(executor(), Queues.<ShardedRequestTask>newConcurrentLinkedQueue(), logger);
            this.idToVersion = idToVersion;
            this.idToPath = idToPath;
            this.versionToState = versionToState;
            this.processors = new MapMaker().softValues().makeMap();
        }
        
        public Logger logger() {
            return logger;
        }

        @Override
        public void handleNotification(Operation.ProtocolResponse<IWatcherEvent> notification) {
            ZNodePath path = ZNodePath.fromString(notification.record().getPath());
            Identifier id = StorageSchema.Safari.Volumes.shardOfPath(path);
            ShardProcessor processor = processors.get(id);
            if (processor != null) {
                processor.responses().handleNotification((Message.ServerResponse<IWatcherEvent>) notification);
            }
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            switch (transition.to()) {
            case ERROR:
                stop();
                break;
            default:
                break;
            }
        }
        
        @Override
        public void onSuccess(PendingShardedTask result) {
            Identifier id = result.getSharded().task().getShard().getValue();
            if (!id.equals(Identifier.zero())) {
                ShardProcessor processor = processors.get(id);
                if (processor != null) {
                    if (!processor.responses().send(result)) {
                        result.getSharded().cancel(true);
                    }
                }
            } else {
                ShardedRequestTask task = result.getSharded();
                Message.ServerResponse<?> response = null;
                try {
                    response = (Message.ServerResponse<?>) result.get();
                } catch (ExecutionException e) {
                    task.setException(e.getCause());
                    onFailure(e.getCause());
                    return;
                } catch (CancellationException e) {
                    task.cancel(true);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                if (! task.isDone()) {
                    task.set(ShardedServerResponseMessage.valueOf(task.task().getShard(), response));
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            ShardedClientExecutor.this.onFailure(t);
        }
        
        @Override
        protected boolean apply(ShardedRequestTask input) throws Exception {
            Identifier id = input.task().getShard().getValue();
            if (id.equals(Identifier.zero())) {
                if (input.task().getRequest().record().opcode() == OpCode.CLOSE_SESSION) {
                    ImmutableList.Builder<ListenableFuture<?>> builder = ImmutableList.builder();
                    for (Map.Entry<Identifier, ShardProcessor> e: processors.entrySet()) {
                        if (e.getValue().requests.isEmpty()) {
                            continue;
                        }
                        SetOnRun<Identifier> promise = SetOnRun.forValue(e.getKey());
                        builder.add(promise);
                        e.getValue().requests.send(promise);
                    }
                    ImmutableList<ListenableFuture<?>> futures = builder.build();
                    if (!futures.isEmpty()) {
                        wait(Futures.allAsList(futures));
                        return false;
                    }
                }
                if (mailbox.remove(input)) {
                    PendingShardedTask task = PendingShardedTask.create(
                            input, 
                            new SoftReference<Message.ClientRequest<?>>(input.task().getRequest()), 
                            SettableFuturePromise.<Message.ServerResponse<?>>create());
                    LoggingFutureListener.listen(logger, task);
                    if (! pending.send(task)) {
                        input.cancel(true);
                        return false;
                    }
                    return true;
                }
                return false;
            } else {
                ShardProcessor processor = processors.get(id);
                if (processor == null) {
                    ListenableFuture<ZNodePath> future = idToPath.apply(id);
                    if (! future.isDone()) {
                        wait(future);
                        return false;
                    }
                    ZNodePath path = future.get();
                    processor = new ShardProcessor(id, path);
                    ShardProcessor existing = processors.put(id, processor);
                    assert (existing == null);
                }
                if (mailbox.remove(input)) {
                    input.processor = processor;
                    if (processor.requests().send(input)) {
                        return true;
                    } else {
                        input.processor = null;
                        input.cancel(false);
                    }
                }
                return false;
            }
        }

        @Override
        protected void doStop() {
            ShardedClientExecutor.this.doStop();
            
            ShardedRequestTask request;
            while ((request = mailbox.poll()) != null) {
                request.cancel(true);
            }
        }
        
        protected final class ShardProcessor {

            private final CachedVolume version;
            private final RequestProcessor requests;
            private final ResponseProcessor responses;
            
            protected ShardProcessor(
                    Identifier id,
                    ZNodePath fromPrefix) {
                assert(!id.equals(Identifier.zero()));
                this.version = idToVersion.apply(id);
                final ZNodePath toPrefix = StorageSchema.Safari.Volumes.Volume.Root.pathOf(id);
                this.requests = new RequestProcessor(MessageRequestPrefixTranslator.forPrefix(fromPrefix, toPrefix));
                this.responses = new ResponseProcessor(MessageResponsePrefixTranslator.forPrefix(toPrefix, fromPrefix));
            }
            
            public RequestProcessor requests() {
                return requests;
            }
            
            public ResponseProcessor responses() {
                return responses;
            }
            
            protected abstract class TaskProcessor<T extends Function<?,?>,U,V> implements Processor<U,V> {

                private final T translator;

                protected TaskProcessor(T translator) {
                    this.translator = translator;
                }

                public T translator() {
                    return translator;
                }
                
            }
            
            protected final class ShardedRequestTaskProcessor extends TaskProcessor<MessageRequestPrefixTranslator, ShardedRequestTask, ListenableFuture<?>> {

                private Optional<VolumeVersionCache.LeasedVersion> lease;
                
                protected ShardedRequestTaskProcessor(
                        MessageRequestPrefixTranslator translator) {
                    super(translator);
                    this.lease = Optional.absent();
                }
                
                @Override
                public ListenableFuture<?> apply(ShardedRequestTask input) throws Exception {
                    Object last = getLastVersion();
                    if (last instanceof ListenableFuture<?>) {
                        return (ListenableFuture<?>) last;
                    }
                    VolumeVersionCache.Version lastVersion = (VolumeVersionCache.Version) last;
                    final int cmp = lastVersion.getVersion().compareTo(input.task().getShard().getVersion());
                    if (cmp < 0) {
                        // I'm out of date
                        version.getLock().readLock().lock();
                        try {
                            Optional<? extends VolumeVersionCache.Version> latest = version.getLatest();
                            if (!latest.isPresent() || latest.get().equals(last)) {
                                return version.getFuture();
                            }
                        } finally {
                            version.getLock().readLock().unlock();
                        }
                        // missed an update, try again
                        return apply(input);
                    } else if ((cmp > 0) || (last instanceof VolumeVersionCache.PastVersion)) {
                        // request is out of date
                        input.setException(new OutdatedVersionException(input.task().getShard()));
                    }
                    final Message.ClientRequest<?> translated = 
                            input.isDone() ? null : translator().apply(input.task().getRequest());
                    return LoggingFutureListener.listen(
                            logger, 
                            PendingShardedTask.create(
                                    input, 
                                    new SoftReference<Message.ClientRequest<?>>(translated), 
                                    SettableFuturePromise.<Message.ServerResponse<?>>create()));
                }
                
                @SuppressWarnings("unchecked")
                protected Object getLastVersion() {
                    VolumeVersionCache.Version last;
                    if (lease.isPresent() && lease.get().getLease().getRemaining() > 0L) {
                        last = lease.get();
                    } else {
                        version.getLock().readLock().lock();
                        try {
                            Optional<? extends VolumeVersionCache.Version> latest = version.getLatest();
                            if (!latest.isPresent()) {
                                return version.getFuture();
                            }
                            if (latest.get() instanceof VolumeVersionCache.LeasedVersion) {
                                if (lease != latest) {
                                    lease = (Optional<VolumeVersionCache.LeasedVersion>) latest;
                                }
                                if (lease.get().getLease().getRemaining() <= 0L) {
                                    return version.getFuture();
                                }
                            } else if (lease.isPresent()) {
                                lease = Optional.absent();
                            }
                            last = latest.get();
                        } finally {
                            version.getLock().readLock().unlock();
                        }
                    }
                    return last;
                }
            }

            protected final class PendingShardedTaskProcessor extends TaskProcessor<MessageResponsePrefixTranslator, PendingShardedTask, ListenableFuture<?>> {

                private Optional<VolumeVersionCache.LeasedVersion> lease;
                
                protected PendingShardedTaskProcessor(
                        MessageResponsePrefixTranslator translator) {
                    super(translator);
                    this.lease = Optional.absent();
                }
                
                @Override
                public ListenableFuture<?> apply(
                        PendingShardedTask input) throws Exception {
                    assert (input.isDone());
                    ShardedRequestTask task = input.getSharded();
                    if (task.isDone()) {
                        return task;
                    } else if (input.isCancelled()) {
                        task.cancel(false);
                        return task;
                    }
                    Message.ServerResponse<?> response;
                    try {
                        response = input.get();
                    } catch (ExecutionException e) {
                        task.setException(e);
                        return task;
                    }
                    Object last = getLastVersion();
                    if (last instanceof ListenableFuture<?>) {
                        return (ListenableFuture<?>) last;
                    }
                    VolumeVersionCache.Version lastVersion = (VolumeVersionCache.Version) last;
                    if (task.task().getShard().getVersion().equals(lastVersion.getVersion())) {
                        if (lastVersion instanceof VolumeVersionCache.LeasedVersion) {
                            if (((VolumeVersionCache.LeasedVersion) lastVersion).getLease().getRemaining() > 0L) {
                                task.set(ShardedServerResponseMessage.valueOf(task.task().getShard(), translator().apply(response)));
                                return task;
                            } else {
                                version.getLock().readLock().lock();
                                try {
                                    Optional<? extends VolumeVersionCache.Version> latest = version.getLatest();
                                    if (!latest.isPresent() || latest.get().equals(lastVersion)) {
                                        return version.getFuture();
                                    }
                                } finally {
                                    version.getLock().readLock().unlock();
                                }
                                // missed an update, try again
                                return apply(input);
                            }
                        }
                    } else {
                        assert (task.task().getShard().getVersion().longValue() < lastVersion.getVersion().longValue());
                    }
                    Object result = null;
                    version.getLock().readLock().lock();
                    try {
                        Optional<? extends VolumeVersionCache.Version> latest = version.getLatest();
                        if (lastVersion.equals(latest.orNull())) {
                            VolumeVersionCache.RecentHistory history = version.getRecentHistory(task.task().getShard().getVersion());
                            assert (!history.getPast().isEmpty());
                            for (VolumeVersionCache.PastVersion version: history.getPast()) {
                                if (response.zxid() < version.getZxids().lowerEndpoint().longValue()) {
                                    result = new OutdatedVersionException(task.task().getShard());
                                    break;
                                } else if (response.zxid() <= version.getZxids().upperEndpoint().longValue()) {
                                    result = version.getVersion();
                                    break;
                                }
                            }
                            if ((result == null) && history.hasLeased()) {
                                if (response.zxid() >= history.getLeased().getXalpha().longValue()) {
                                    if (history.getLeased().getLease().getRemaining() <= 0L) {
                                        return version.getFuture();
                                    }
                                    result = history.getLeased().getVersion();
                                }
                            }  
                            if (result == null) {
                                result = new OutdatedVersionException(task.task().getShard());
                            }
                        }
                    } finally {
                        version.getLock().readLock().unlock();
                    }
                    if (result == null) {
                        // missed an update, try again
                        return apply(input);
                    }
                    if (result instanceof UnsignedLong) {
                        UnsignedLong v = (UnsignedLong) result;
                        if (v.longValue() > task.task().getShard().getVersion().longValue()) {
                            ZNodeName remaining = ZNodePath.fromString(((Records.PathGetter) task.task().getRequest().record()).getPath()).suffix(translator().getPrefix().from());
                            if (!(remaining instanceof EmptyZNodeLabel)) {
                                ListenableFuture<? extends VolumeVersion<?>> state = versionToState.apply(VersionedId.valueOf(v, version.getId()));
                                if (!state.isDone()) {
                                    return state;
                                }
                                if (!VolumeBranches.contains(((AssignedVolumeBranches) state.get()).getState().getValue().getBranches().keySet(), remaining)) {
                                    result = new OutdatedVersionException(task.task().getShard());
                                }
                            }
                        } else { 
                            assert (v.equals(task.task().getShard().getVersion()));
                        }
                    }
                    if (!task.isDone()) {
                        if (result instanceof UnsignedLong) { 
                            task.set(ShardedServerResponseMessage.valueOf(VersionedId.valueOf((UnsignedLong) result, version.getId()), translator().apply(response)));
                        } else {
                            task.setException((Exception) result);
                        }
                    }
                    return task;
                }
                
                @SuppressWarnings("unchecked")
                protected Object getLastVersion() {
                    VolumeVersionCache.Version last;
                    if (lease.isPresent() && lease.get().getLease().getRemaining() > 0L) {
                        last = lease.get();
                    } else {
                        version.getLock().readLock().lock();
                        try {
                            Optional<? extends VolumeVersionCache.Version> latest = version.getLatest();
                            if (!latest.isPresent()) {
                                return version.getFuture();
                            }
                            if (latest.get() instanceof VolumeVersionCache.LeasedVersion) {
                                if (lease != latest) {
                                    lease = (Optional<VolumeVersionCache.LeasedVersion>) latest;
                                }
                            } else if (lease.isPresent()) {
                                lease = Optional.absent();
                            }
                            last = latest.get();
                        } finally {
                            version.getLock().readLock().unlock();
                        }
                    }
                    return last;
                }
            }

            protected abstract class TaskProcessorActor<V, T extends TaskProcessor<?,V,?>> extends WaitingActor<ListenableFuture<?>,ListenableFuture<?>> {

                private final T processor;
                
                protected TaskProcessorActor(
                        T processor) {
                    super(executor(), Queues.<ListenableFuture<?>>newConcurrentLinkedQueue(), logger());
                    this.processor = processor;
                }
                
                protected T processor() {
                    return processor;
                }
            }

            protected final class RequestProcessor extends TaskProcessorActor<ShardedRequestTask, ShardedRequestTaskProcessor> {

                protected RequestProcessor(MessageRequestPrefixTranslator translator) {
                    super(new ShardedRequestTaskProcessor(translator));
                }
                
                public boolean isEmpty() {
                    return mailbox.isEmpty();
                }
                
                @Override
                protected boolean apply(ListenableFuture<?> input)
                        throws Exception {
                    if (input instanceof ShardedRequestTask) {
                        final ShardedRequestTask task = (ShardedRequestTask) input;
                        ListenableFuture<?> future = processor().apply(task);
                        if (future instanceof PendingShardedTask) {
                            if (mailbox.remove(input)) {
                                if (pending.send((PendingShardedTask) future)) {
                                    return true;
                                }
                                task.processor = null;
                                input.cancel(false);
                            }
                        } else {
                            wait(future);
                        }
                        return false;
                    } else {
                        if (mailbox.remove(input)) {
                            ((Runnable) input).run();
                            return true;
                        }
                        return false;
                    }
                }
            }
            
            protected final class ResponseProcessor extends TaskProcessorActor<PendingShardedTask, PendingShardedTaskProcessor> {

                protected ResponseProcessor(
                        MessageResponsePrefixTranslator translator) {
                    super(new PendingShardedTaskProcessor(translator));
                }
                
                public void handleNotification(Message.ServerResponse<IWatcherEvent> notification) {
                    Notification task = LoggingFutureListener.listen(logger, 
                            new Notification(notification, 
                                    SettableFuturePromise.<Optional<ShardedServerResponseMessage<IWatcherEvent>>>create()));
                    task.run();
                    send(task);
                }

                @SuppressWarnings("unchecked")
                @Override
                protected boolean apply(ListenableFuture<?> input) throws Exception {
                    if (input instanceof PendingShardedTask) {
                        assert (input.isDone());
                        PendingShardedTask task = (PendingShardedTask) input;
                        ListenableFuture<?> future = processor().apply(task);
                        if (!(future instanceof ShardedRequestTask)) {
                            wait(future);
                            return false;
                        }
                        task.getSharded().processor = null;
                    } else {
                        Notification notification = (Notification) input;
                        if (!notification.isDone()) {
                            wait(notification);
                            return false;
                        }
                        Optional<ShardedServerResponseMessage<IWatcherEvent>> response = notification.get();
                        if (response.isPresent()) {
                            listeners.handleNotification(response.get());
                        }
                    }
                    return mailbox.remove(input);
                }
                
                protected final class Notification extends PromiseTask<Message.ServerResponse<IWatcherEvent>, Optional<ShardedServerResponseMessage<IWatcherEvent>>> implements Runnable, FutureCallback<Boolean> {
                    
                    public Notification(
                            Message.ServerResponse<IWatcherEvent> task,
                            Promise<Optional<ShardedServerResponseMessage<IWatcherEvent>>> delegate) {
                        super(task, delegate);
                    }
                    
                    @Override
                    public void run() {
                        version.getLock().readLock().lock();
                        try {
                            if (isDone()) {
                                return;
                            }
                            if (version.hasPendingVersion()) {
                                version.getFuture().addListener(this, MoreExecutors.directExecutor());
                                return;
                            }
                            Iterator<VersionedValue<VolumeVersionCache.VersionState>> states = version.getVersionState().iterator();
                            switch (states.next().getValue()) {
                            case UNKNOWN:
                                break;
                            case XALPHA:
                            {
                                onSuccess(Boolean.TRUE);
                                return;
                            }
                            case XOMEGA:
                            {
                                onSuccess(Boolean.FALSE);
                                return;
                            }
                            default:
                                throw new AssertionError();
                            }
                            if (!states.hasNext()) {
                                onSuccess(Boolean.TRUE);
                            }
                            switch (states.next().getValue()) {
                            case XALPHA:
                            {
                                onSuccess(Boolean.TRUE);
                                return;
                            }
                            case XOMEGA:
                            {
                                ZNodeName remaining = ZNodePath.fromString(task().record().getPath()).suffix(processor().translator().getPrefix().from());
                                if (!(remaining instanceof EmptyZNodeLabel)) {
                                    try {
                                        Futures.addCallback(
                                                Futures.transform(
                                                versionToState.apply(
                                                        VersionedId.valueOf(version.getVersionState().getFirst().getVersion(), version.getId())),
                                                        new Contains(remaining)), 
                                                this);
                                    } catch (Exception e) {
                                        setException(e);
                                    }
                                } else {
                                    onSuccess(Boolean.TRUE);
                                }
                                break;
                            }
                            default:
                                throw new AssertionError();
                            }
                        } finally {
                            version.getLock().readLock().unlock();
                        }
                    }

                    @Override
                    public void onSuccess(Boolean result) {
                        @SuppressWarnings("unchecked")
                        Optional<ShardedServerResponseMessage<IWatcherEvent>> translated = result.booleanValue() ? 
                                Optional.of(ShardedServerResponseMessage.valueOf(VersionedId.valueOf(UnsignedLong.ZERO, version.getId()), (Message.ServerResponse<IWatcherEvent>) processor().translator().apply(task()))) :
                                Optional.<ShardedServerResponseMessage<IWatcherEvent>>absent();
                        set(translated);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        setException(t);
                    }
                    
                    private final class Contains implements Function<VolumeVersion<?>, Boolean> {

                        private final ZNodeName remaining;
                        
                        private Contains(ZNodeName remaining) {
                            this.remaining = remaining;
                        }
                        
                        @Override
                        public Boolean apply(VolumeVersion<?> input) {
                            return Boolean.valueOf(VolumeBranches.contains(((AssignedVolumeBranches) input).getState().getValue().getBranches().keySet(), remaining));
                        }
                    }
                }
            }
        }
    }
}
