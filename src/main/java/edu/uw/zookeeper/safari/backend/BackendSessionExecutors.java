package edu.uw.zookeeper.safari.backend;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.Message.ClientSession;
import edu.uw.zookeeper.protocol.Operation.Response;
import edu.uw.zookeeper.protocol.client.ConnectTask;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.safari.control.volumes.VolumeBranchesCache;
import edu.uw.zookeeper.safari.control.volumes.VolumeDescriptorCache;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.snapshot.SnapshotSessionExecutors;
import edu.uw.zookeeper.safari.storage.volumes.VolumeVersionCache;

public final class BackendSessionExecutors extends AbstractIdleService implements TaskExecutor<MessageSessionOpenRequest, MessageSessionOpenResponse>, Iterable<BackendSessionExecutors.BackendSessionExecutor> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        protected Module() {}

        @Provides @Singleton
        public Function<ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> newConnectToShardedClientExecutor(
                final VolumeVersionCache versions,
                final VolumeDescriptorCache descriptors,
                final VolumeBranchesCache branches,
                final ScheduledExecutorService scheduler) {
            return new Function<ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>(){
                @Override
                public ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>> apply(ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>> input) {
                    return ShardedClientExecutor.fromConnect(
                            versions.volumes(),
                            branches,
                            descriptors.descriptors().lookup(),
                            input,
                            scheduler);
                }
            };
        }
        
        @Provides @Singleton
        public AsyncFunction<MessageSessionOpenRequest, ShardedClientExecutor<?>> newShardedClientExecutorFactory(
                final AsyncFunction<MessageSessionOpenRequest, MessageSessionOpenRequest> openToRequest,
                final AsyncFunction<MessageSessionOpenRequest, ? extends ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> openToConnect,
                final Function<ConnectTask<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> connectToClient) {
            return new AsyncFunction<MessageSessionOpenRequest, ShardedClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>() {
                @Override
                public ListenableFuture<ShardedClientExecutor<? extends ProtocolConnection<? super ClientSession, ? extends Response, ?, ?, ?>>> apply(
                        final MessageSessionOpenRequest request) throws Exception {
                    return Futures.transform(
                            Futures.transform(
                                openToRequest.apply(request), 
                                openToConnect),
                            connectToClient);
                }
            };
        } 
        
        @Provides @Singleton
        public BackendSessionExecutors newBackendSessionExecutors(
                AsyncFunction<MessageSessionOpenRequest, ShardedClientExecutor<?>> clientFactory,
                SchemaClientService<StorageZNode<?>,?> storage,
                ServiceMonitor monitor) throws Exception {
            final BackendSessionExecutors instance = BackendSessionExecutors.create(
                    clientFactory);
            monitor.add(instance);
            SnapshotSessionExecutors.listen(
                    instance, 
                    storage, 
                    new Function<Long, ShardedClientExecutor<?>>() {
                        @Override
                        public ShardedClientExecutor<?> apply(Long input) {
                            BackendSessionExecutor executor = instance.get(input);
                            return (executor == null) ? null : executor.client();
                        }
                    });
            return instance;
        }
        
        @Override
        protected void configure() {
        }
    }

    public static BackendSessionExecutors create(
            AsyncFunction<MessageSessionOpenRequest,ShardedClientExecutor<?>> clientFactory) {
        return new BackendSessionExecutors(clientFactory, new MapMaker().<Long,BackendSessionExecutor>makeMap());
    }
    
    protected final AsyncFunction<MessageSessionOpenRequest,ShardedClientExecutor<?>> clientFactory;
    protected final ConcurrentMap<Long, BackendSessionExecutor> sessions;
    
    protected BackendSessionExecutors(
            AsyncFunction<MessageSessionOpenRequest,ShardedClientExecutor<?>> clientFactory,
            ConcurrentMap<Long, BackendSessionExecutor> sessions) {
        super();
        this.clientFactory = clientFactory;
        this.sessions = sessions;
    }
    
    public BackendSessionExecutor get(Long session) {
        return sessions.get(session);
    }
    
    @Override
    public ListenableFuture<MessageSessionOpenResponse> submit(MessageSessionOpenRequest request) {
        if (!isRunning()) {
            throw new RejectedExecutionException();
        }
        BackendSessionExecutor executor = get(request.getIdentifier());
        if (executor != null) {
            return Futures.immediateFuture(
                    MessageSessionOpenResponse.of(request.getIdentifier(),
                        Futures.getUnchecked(executor.client().session())));
        }
        final Promise<MessageSessionOpenResponse> promise = SettableFuturePromise.create();
        try {
            new SessionOpen(request, clientFactory.apply(request), promise);
        } catch (Exception e) {
            throw new RejectedExecutionException(e);
        }
        return promise;
    }
    
    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        for (BackendSessionExecutor e: Iterables.consumingIterable(this)) {
            e.stop();
        }
    }

    @Override
    public Iterator<BackendSessionExecutor> iterator() {
        return sessions.values().iterator();
    }

    protected final class SessionOpen implements Runnable, Callable<Optional<MessageSessionOpenResponse>> {
        
        private final MessageSessionOpenRequest request;
        private final ListenableFuture<? extends ShardedClientExecutor<?>> future;
        private final CallablePromiseTask<SessionOpen,MessageSessionOpenResponse> delegate;
        
        public SessionOpen(
                MessageSessionOpenRequest request, 
                ListenableFuture<? extends ShardedClientExecutor<?>> future, 
                Promise<MessageSessionOpenResponse> promise) {
            this.request = request;
            this.future = future;
            this.delegate = CallablePromiseTask.create(this, promise);
            future.addListener(this, MoreExecutors.directExecutor());
        }

        @Override
        public void run() {
            delegate.run();
        }
        
        @Override
        public Optional<MessageSessionOpenResponse> call() throws Exception {
            if (future.isDone()) {
                final Long session = request.getIdentifier();
                final ShardedClientExecutor<?> client = future.get();
                assert (client.session().isDone());
                final ConnectMessage.Response connected = client.session().get();
                final BackendSessionExecutor executor = new BackendSessionExecutor(session, client);
                BackendSessionExecutor existing = sessions.put(session, executor);
                if (existing != null) {
                    existing.stop();
                }
                switch (client.connection().codec().state()) {
                case DISCONNECTED:
                case ERROR:
                    executor.stop();
                    break;
                default:
                    break;
                }
                if (!isRunning()) {
                    executor.stop();
                }
                return Optional.of(MessageSessionOpenResponse.of(session, connected));
            }
            return Optional.absent();
        }
    }
    
    public final class BackendSessionExecutor extends AbstractPair<Long, ShardedClientExecutor<?>> implements SessionListener {
        
        public BackendSessionExecutor(
                Long session,
                ShardedClientExecutor<?> client) {
            super(session, client);
            client().subscribe(this);
        }
        
        public Long session() {
            return first;
        }
        
        public ShardedClientExecutor<?> client() {
            return second;
        }
        
        public void stop() {
            sessions.remove(session(), this);
            client().unsubscribe(this);
            client().stop();
        }

        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            switch (transition.to()) {
            case DISCONNECTED:
            case ERROR:
                sessions.remove(session(), this);
                client().unsubscribe(this);
                break;
            default:
                break;
            }
        }

        @Override
        public void handleNotification(
                Operation.ProtocolResponse<IWatcherEvent> notification) {
        }
    }
}
