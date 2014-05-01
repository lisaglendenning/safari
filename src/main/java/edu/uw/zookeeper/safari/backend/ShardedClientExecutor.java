package edu.uw.zookeeper.safari.backend;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Queues;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
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
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.common.WaitingActor;
import edu.uw.zookeeper.safari.data.EmptyVolume;
import edu.uw.zookeeper.safari.data.VersionTransition;
import edu.uw.zookeeper.safari.data.VersionedId;
import edu.uw.zookeeper.safari.data.VersionedVolume;
import edu.uw.zookeeper.safari.data.Volume;
import edu.uw.zookeeper.safari.peer.protocol.ShardedClientRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;

public class ShardedClientExecutor<C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> extends PendingQueueClientExecutor<ShardedClientRequestMessage<?>, ShardedServerResponseMessage<?>, ShardedClientExecutor.ShardedRequestTask, C, ShardedClientExecutor.PendingShardedTask> {

    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> ShardedClientExecutor<C> newInstance(
            Function<Identifier, ? extends Supplier<VersionTransition>> idToVersion,
            Function<Identifier, ? extends AsyncFunction<Long,UnsignedLong>> idToZxid,
            Function<Identifier, ? extends AsyncFunction<UnsignedLong,Volume>> idToVolume,
            AsyncFunction<Identifier, ZNodePath> idToPath,
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService scheduler) {
        return newInstance(
                idToVersion,
                idToZxid,
                idToVolume,
                idToPath,
                ConnectTask.connect(request, connection),
                connection,
                TimeValue.milliseconds(request.getTimeOut()),
                scheduler);
    }

    public static <C extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> ShardedClientExecutor<C> newInstance(
            Function<Identifier, ? extends Supplier<VersionTransition>> idToVersion,
            Function<Identifier, ? extends AsyncFunction<Long,UnsignedLong>> idToZxid,
            Function<Identifier, ? extends AsyncFunction<UnsignedLong,Volume>> idToVolume,
            AsyncFunction<Identifier, ZNodePath> idToPath,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        return new ShardedClientExecutor<C>(
                idToVersion,
                idToZxid,
                idToVolume,
                idToPath,
                LogManager.getLogger(ShardedClientExecutor.class),
                session, 
                connection,
                timeOut,
                scheduler);
    }

    protected final Function<Identifier, ? extends Supplier<VersionTransition>> idToVersion;
    protected final Function<Identifier, ? extends AsyncFunction<Long,UnsignedLong>> idToZxid;
    protected final Function<Identifier, ? extends AsyncFunction<UnsignedLong,Volume>> idToVolume;
    protected final AsyncFunction<Identifier, ZNodePath> idToPath;
    protected final Tasks actor;
    
    protected ShardedClientExecutor(
            Function<Identifier, ? extends Supplier<VersionTransition>> idToVersion,
            Function<Identifier, ? extends AsyncFunction<Long,UnsignedLong>> idToZxid,
            Function<Identifier, ? extends AsyncFunction<UnsignedLong,Volume>> idToVolume,
            AsyncFunction<Identifier, ZNodePath> idToPath,
            Logger logger,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        super(logger, session, connection, timeOut, scheduler);
        this.idToVersion = idToVersion;
        this.idToZxid = idToZxid;
        this.idToVolume = idToVolume;
        this.idToPath = idToPath;
        this.actor = new Tasks(logger);
    }

    @Override
    public ListenableFuture<ShardedServerResponseMessage<?>> submit(
            ShardedClientRequestMessage<?> request, Promise<ShardedServerResponseMessage<?>> promise) {
        ShardedRequestTask task = new ShardedRequestTask(
                request, 
                LoggingPromise.create(logger(), promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
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
    protected Tasks actor() {
        return actor;
    }

    @Override
    protected Logger logger() {
        return actor.logger();
    }

    protected static class ShardedRequestTask extends AbstractConnectionClientExecutor.RequestTask<ShardedClientRequestMessage<?>, ShardedServerResponseMessage<?>> {

        public ShardedRequestTask(
                ShardedClientRequestMessage<?> task,
                Promise<ShardedServerResponseMessage<?>> promise) {
            super(task, promise);
            assert(task.xid() != OpCodeXid.PING.xid());
        }
    }
    
    public static class PendingShardedTask extends PendingTask {

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

    protected class Tasks extends WaitingActor<ListenableFuture<?>,ShardedRequestTask> implements FutureCallback<PendingShardedTask>, SessionListener {

        private final ConcurrentMap<Identifier, ShardProcessor> processors;
        
        protected Tasks(Logger logger) {
            super(executor(), Queues.<ShardedRequestTask>newConcurrentLinkedQueue(), logger);
            this.processors = new MapMaker().makeMap();
        }
        
        public Logger logger() {
            return logger;
        }

        @Override
        public void handleNotification(Operation.ProtocolResponse<IWatcherEvent> notification) {
            ZNodePath path = ZNodePath.fromString(notification.record().getPath());
            Identifier id = BackendSchema.Safari.Volumes.shardOfPath(path);
            ShardProcessor processor = processors.get(id);
            if (processor != null) {
                processor.responses.send(Futures.immediateFuture(notification));
            }
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }
        
        @Override
        public void onSuccess(PendingShardedTask result) {
            Identifier id = result.getSharded().task().getShard().getIdentifier();
            ShardProcessor processor = processors.get(id);
            if (processor != null) {
                if (!processors.get(id).responses.send(result)) {
                    result.getSharded().cancel(true);
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            // TODO Auto-generated method stub
        }
        
        @Override
        protected boolean apply(ShardedRequestTask input) throws Exception {
            Identifier id = input.task().getShard().getIdentifier();
            if (id.equals(Identifier.zero())) {
                ImmutableList.Builder<Promise<Identifier>> builder = ImmutableList.builder();
                for (Map.Entry<Identifier, ShardProcessor> e: processors.entrySet()) {
                    if (e.getKey().equals(id) || e.getValue().requests.isEmpty()) {
                        continue;
                    }
                    Promise<Identifier> promise = SettableFuturePromise.create();
                    builder.add(promise);
                    e.getValue().requests.send(promise);
                }
                ImmutableList<Promise<Identifier>> futures = builder.build();
                if (!futures.isEmpty()) {
                    wait(Futures.allAsList(futures));
                    return false;
                }
            }
            ShardProcessor processor = processors.get(id);
            if (processor == null) {
                ListenableFuture<ZNodePath> future = idToPath.apply(id);
                if (! future.isDone()) {
                    wait(future);
                    return false;
                }
                ZNodePath path = future.get();
                Supplier<VersionTransition> version = idToVersion.apply(id);
                AsyncFunction<Long,UnsignedLong> zxid = idToZxid.apply(id);
                AsyncFunction<UnsignedLong,Volume> volume = idToVolume.apply(id);
                processor = new ShardProcessor(id, path, version, zxid, volume);
                ShardProcessor existing = processors.putIfAbsent(id, processor);
                if (existing != null) {
                    processor = existing;
                }
            }
            if (! mailbox.remove(input)) {
                return false;
            }
            if (! processor.requests.send(input)) {
                return false;
            }
            return true;
        }
        
        // TODO: delete processors for transferred volumes
        protected class ShardProcessor {

            private final Identifier identifier;
            private final Supplier<VersionTransition> version;
            private final AsyncFunction<Long,UnsignedLong> zxid;
            private final AsyncFunction<UnsignedLong,? extends VersionedVolume> volume;
            private final RequestProcessor requests;
            private final ResponseProcessor responses;
            
            public ShardProcessor(
                    Identifier identifier,
                    ZNodePath fromPrefix,
                    Supplier<VersionTransition> version,
                    AsyncFunction<Long,UnsignedLong> zxid,
                    AsyncFunction<UnsignedLong,? extends VersionedVolume> volume) {
                this.identifier = identifier;
                this.version = version;
                this.zxid = zxid;
                this.volume = volume;
                ZNodePath toPrefix = identifier.equals(Identifier.zero()) ? ZNodePath.root() : BackendSchema.Safari.Volumes.Volume.Root.pathOf(identifier);
                this.requests = new RequestProcessor(MessageRequestPrefixTranslator.forPrefix(fromPrefix, toPrefix));
                this.responses = new ResponseProcessor(MessageResponsePrefixTranslator.forPrefix(toPrefix, fromPrefix));
            }

            protected class RequestProcessor extends WaitingActor<ListenableFuture<?>,Promise<?>> {

                private final MessageRequestPrefixTranslator translator;
                
                protected RequestProcessor(MessageRequestPrefixTranslator translator) {
                    super(executor(), Queues.<Promise<?>>newConcurrentLinkedQueue(), logger());
                    this.translator = translator;
                }
                
                public boolean isEmpty() {
                    return mailbox.isEmpty();
                }

                @SuppressWarnings("unchecked")
                @Override
                protected boolean apply(Promise<?> input)
                        throws Exception {
                    if (input instanceof ShardedRequestTask) {
                        VersionTransition transition = version.get();
                        if (!transition.getCurrent().isPresent() && !transition.getNext().isDone()) {
                            wait(transition.getNext());
                            return false;
                        }
                        UnsignedLong current = transition.getCurrent().get();
                        if (current == null) {
                            try {
                                current = transition.getNext().get();
                            } catch (ExecutionException e) {
                                input.setException(e.getCause());
                            } catch (CancellationException e) {
                                input.cancel(true);
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            }
                        }
                        final ShardedRequestTask request = (ShardedRequestTask) input;
                        final int cmp = (current == null) ? 1 : current.compareTo(request.task().getShard().getVersion());
                        if (cmp >= 0) {
                            if (!mailbox.remove(input)) {
                                return false;
                            }
                            Message.ClientRequest<?> translated = null;
                            if (! input.isDone()) {
                                if (cmp == 0) {
                                    // green light
                                    translated = translator.apply(request.task().getRequest());
                                } else {
                                    // request is out of date
                                    input.setException(new OutdatedVersionException(request.task().getShard()));
                                }
                            }
                            PendingShardedTask task = PendingShardedTask.create(request, new SoftReference<Message.ClientRequest<?>>(translated), SettableFuturePromise.<Message.ServerResponse<?>>create());
                            if (! pending.send(task)) {
                                input.cancel(true);
                                return false;
                            }
                            return true;
                        } else {
                            // I am out of date
                            wait(version.get().getNext());
                            return false;
                        }
                    } else {
                        ((Promise<Identifier>) input).set(identifier);
                        return mailbox.remove(input);
                    }
                }
            }
            
            protected class ResponseProcessor extends WaitingActor<ListenableFuture<?>,ListenableFuture<?>> {

                private final MessageResponsePrefixTranslator translator;
                
                protected ResponseProcessor(
                        MessageResponsePrefixTranslator translator) {
                    super(executor(), Queues.<ListenableFuture<?>>newConcurrentLinkedQueue(), logger());
                    this.translator = translator;
                }

                @SuppressWarnings("unchecked")
                @Override
                protected boolean apply(ListenableFuture<?> input) throws Exception {
                    assert (input.isDone());
                    VersionTransition transition = version.get();
                    if (!transition.getCurrent().isPresent() && !transition.getNext().isDone()) {
                        wait(transition.getNext());
                        return false;
                    }
                    UnsignedLong current = transition.getCurrent().get();
                    if (input instanceof PendingShardedTask) {
                        ShardedRequestTask task = ((PendingShardedTask) input).getSharded();
                        Message.ServerResponse<?> response = null;
                        try {
                            response = (Message.ServerResponse<?>) input.get();
                        } catch (ExecutionException e) {
                            task.setException(e.getCause());
                            onFailure(e.getCause());
                            return false;
                        } catch (CancellationException e) {
                            task.cancel(true);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        if (! task.isDone()) {
                            if (current == null) {
                                try {
                                    current = transition.getNext().get();
                                } catch (ExecutionException e) {
                                    if (!(e.getCause() instanceof SafariException)) {
                                        task.setException(e.getCause());
                                        onFailure(e.getCause());
                                        return false;
                                    }
                                } catch (CancellationException e) {
                                    task.cancel(true);
                                } catch (InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                            }
                            
                            if (! task.isDone()) {
                                if (!Objects.equal(current, task.task().getShard().getVersion())) {
                                    assert ((current == null) || (current.longValue() > task.task().getShard().getVersion().longValue()));
                                    ZNodePath path = ZNodePath.fromString(((Records.PathGetter) task.task().record()).getPath());
                                    try {
                                        Optional<UnsignedLong> version = validate(response.zxid(), path);
                                        if (!version.isPresent()) {
                                            return false;
                                        }
                                    } catch (SafariException e) {
                                        task.setException(e);
                                    }
                                }
                                if (! task.isDone()) {
                                    task.set(ShardedServerResponseMessage.valueOf(task.task().getShard(), translator.apply(response)));
                                }
                            }
                        }
                    } else {
                        // notification
                        Message.ServerResponse<?> response = (Message.ServerResponse<?>) Futures.getUnchecked(input);
                        if (current == null) {
                            try {
                                current = transition.getNext().get();
                            } catch (ExecutionException e) {
                                if (! (e.getCause() instanceof SafariException)) {
                                    onFailure(e.getCause());
                                    return false;                                    
                                }
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            } catch (Exception e) {
                                onFailure(e);
                                return false;
                            }
                        }

                        // we don't have much context for notifications
                        // so check every notification for validity
                        ZNodePath path = ZNodePath.fromString(((Records.PathGetter) response.record()).getPath());
                        Optional<UnsignedLong> version;
                        try {
                            version = validate(response.zxid(), path);
                        } catch (SafariException e) {
                            return mailbox.remove(input);
                        }
                        if (!version.isPresent()) {
                            return false;
                        }
                        listeners.handleNotification(ShardedServerResponseMessage.valueOf(VersionedId.valueOf(identifier, version.get()), (Message.ServerResponse<IWatcherEvent>) translator.apply(response)));
                    }
                    return mailbox.remove(input);
                }
                
                protected Optional<UnsignedLong> validate(long zxid, ZNodePath path) throws Exception {
                    ListenableFuture<UnsignedLong> versionFuture = ShardProcessor.this.zxid.apply(zxid);
                    if (!versionFuture.isDone()) {
                        wait(versionFuture);
                        return Optional.absent();
                    }
                    UnsignedLong version = versionFuture.get();
                    ListenableFuture<? extends VersionedVolume> volumeFuture = ShardProcessor.this.volume.apply(version);
                    if (!volumeFuture.isDone()) {
                        wait(volumeFuture);
                        return Optional.absent();
                    }
                    VersionedVolume volume = volumeFuture.get();
                    if (volume instanceof EmptyVolume) {
                        throw new OutdatedVersionException(VersionedId.valueOf(volume.getDescriptor().getId(), volume.getVersion()));
                    }
                    
                    ZNodePath prefix = volume.getDescriptor().getPath();
                    ZNodeName remaining = path.suffix((prefix.isRoot()) ? 0: prefix.length());
                    for (ZNodeName branch: ((Volume) volume).getBranches().keySet()) {
                        if (remaining.startsWith(branch)) {
                            throw new OutdatedVersionException(VersionedId.valueOf(volume.getDescriptor().getId(), volume.getVersion()));
                        }
                    }
                    return Optional.of(version);
                }
            }
        }
    }
}
