package edu.uw.zookeeper.safari.volumes;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.QueryZKLeader;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.FutureChain.FutureDequeChain;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.AbstractConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.volumes.VolumeDescriptorCache;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationDirective;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.volumes.AssignParameters;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.snapshot.CoordinateSnapshot;
import edu.uw.zookeeper.safari.storage.snapshot.ExecuteSnapshot;
import edu.uw.zookeeper.safari.storage.snapshot.SnapshotClientModule;
import edu.uw.zookeeper.safari.storage.snapshot.SnapshotVolumeModule;

public final class VolumeOperationExecutor<T extends ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?>> extends LoggingServiceListener<VolumeOperationExecutor<T>> implements TaskExecutor<VolumeOperationDirective, Boolean> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}

        @Provides
        public VolumeOperationExecutor<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> newVolumeExecutor(
                final Injector injector,
                final @Control Materializer<ControlZNode<?>,?> control,
                final VolumeDescriptorCache descriptors,
                final @Region Identifier region,
                final @Storage Materializer<StorageZNode<?>,?> materializer,
                final @Storage ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> storage,
                final @Storage ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
                final @Storage TimeValue timeOut,
                final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous,
                final ScheduledExecutorService scheduler,
                final RegionRoleService service) {
            Services.startAndWait(anonymous);
            final RegionToEnsemble regionToEnsemble = RegionToEnsemble.create(
                    control);
            final EnsembleToLeader ensembleToLeader = EnsembleToLeader.create(
                    anonymous);
            final Supplier<ListenableFuture<Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>> localConnection = new Supplier<ListenableFuture<Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>>() {
                @Override
                public ListenableFuture<Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>> get() {
                    return Futures.transform(
                            ConnectedCallback.create(storage.get()),
                            new Function<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>,Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>(){
                                @Override
                                public Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> apply(
                                        OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>> input) {
                                    return Pair.create(storage.first(), input);
                                }
                            });
                }
            };
            final AsyncFunction<ServerInetAddressView, Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> clients = 
                    new AsyncFunction<ServerInetAddressView, Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>>() {
                        @Override
                        public ListenableFuture<Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>> apply(
                                final ServerInetAddressView server) throws Exception {
                            return Futures.transform(
                                    Futures.transform(
                                            connections.connect(server.get()), 
                                            new AsyncFunction<ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>,OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>(){
                                                @Override
                                                public ListenableFuture<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> apply(ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?> input) {
                                                    return ConnectedCallback.create(
                                                            Futures.<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>immediateFuture(
                                                                OperationClientExecutor.newInstance(
                                                                ConnectMessage.Request.NewRequest.newInstance(timeOut, 0L), 
                                                                input, 
                                                                scheduler)));
                                                }                                                
                                            }),
                                    new Function<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>, Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>() {
                                        @Override
                                        public Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> apply(OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>> input) {
                                            return Pair.create(server, input);
                                        }
                                    });
                        }
                };
                VolumeOperationExecutor<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> instance = VolumeOperationExecutor.create(
                    control,
                    descriptors.descriptors().lookup(),
                    VolumeToRegion.create(control),
                    regionToEnsemble,
                    new AsyncFunction<Identifier, Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>() {
                        @Override
                        public ListenableFuture<Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>> apply(
                                final Identifier input) throws Exception {
                            if (input.equals(region)) {
                                return localConnection.get();
                            } else {
                                return Futures.transform(
                                        Futures.transform(
                                            regionToEnsemble.apply(input),
                                            ensembleToLeader),
                                        clients);
                            }
                        }
                    },
                    new AsyncFunction<Identifier, Materializer<StorageZNode<?>,?>>() {
                        @Override
                        public ListenableFuture<Materializer<StorageZNode<?>, ?>> apply(
                                Identifier input) throws Exception {
                            checkArgument(input.equals(region));
                            return Futures.<Materializer<StorageZNode<?>, ?>>immediateFuture(materializer);
                        }
                    },
                    InjectingSnapshotProcessor.create(injector));
                Services.listen(instance, service);
                return instance;
        }

        @Override
        public Key<? extends Service.Listener> getKey() {
            return Key.get(new TypeLiteral<VolumeOperationExecutor<?>>(){});
        }

        @Override
        protected void configure() {
            bind(new TypeLiteral<VolumeOperationExecutor<?>>(){}).to(new TypeLiteral<VolumeOperationExecutor<OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>() {});
            bind(new TypeLiteral<TaskExecutor<VolumeOperationDirective, Boolean>>(){}).to(new TypeLiteral<VolumeOperationExecutor<?>>(){});
        }
    }
    
    public static class OperationModule extends AbstractModule {
        public static OperationModule create(
                VolumeOperation<?> operation) {
            return new OperationModule(operation);
        }
        
        private final VolumeOperation<?> operation;
        
        protected OperationModule(
                VolumeOperation<?> operation) {
            this.operation = operation;
        }

        @Override
        protected void configure() {
            bind(new TypeLiteral<VolumeOperation<?>>(){}).toInstance(operation);
        }
    }
    
    public static <T extends ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?>> VolumeOperationExecutor<T> create(
            Materializer<ControlZNode<?>,?> control,
            AsyncFunction<Identifier, ZNodePath> volumeToPath,
            AsyncFunction<VersionedId, Identifier> volumeToRegion,
            AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> regionToEnsemble,
            AsyncFunction<Identifier, ? extends Pair<ServerInetAddressView, ? extends T>> regionToClient,
            AsyncFunction<Identifier, ? extends Materializer<StorageZNode<?>,?>> regionToMaterializer,
            ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureDequeChain<ListenableFuture<?>>> snapshot) {
        return new VolumeOperationExecutor<T>(
                control,
                volumeToPath,
                volumeToRegion,
                regionToEnsemble,
                regionToClient,
                regionToMaterializer,
                snapshot);
    }
    
    private final ConcurrentMap<VolumeLogEntryPath, VolumeOperationTask> requests;
    private final Materializer<ControlZNode<?>,?> control;
    private final AsyncFunction<Identifier, ZNodePath> volumeToPath;
    private final AsyncFunction<VersionedId, Identifier> volumeToRegion;
    private final AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> regionToEnsemble;
    private final AsyncFunction<Identifier, ? extends Pair<ServerInetAddressView, ? extends T>> regionToClient;
    private final AsyncFunction<Identifier, ? extends Materializer<StorageZNode<?>,?>> regionToMaterializer;
    private final ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureDequeChain<ListenableFuture<?>>> snapshot;
    
    protected VolumeOperationExecutor(
            Materializer<ControlZNode<?>,?> control,
            AsyncFunction<Identifier, ZNodePath> volumeToPath,
            AsyncFunction<VersionedId, Identifier> volumeToRegion,
            AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> regionToEnsemble,
            AsyncFunction<Identifier, ? extends Pair<ServerInetAddressView, ? extends T>> regionToClient,
            AsyncFunction<Identifier, ? extends Materializer<StorageZNode<?>,?>> regionToMaterializer,
            ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureDequeChain<ListenableFuture<?>>> snapshot) {
        this.control = control;
        this.volumeToPath = volumeToPath;
        this.volumeToRegion = volumeToRegion;
        this.regionToEnsemble = regionToEnsemble;
        this.regionToClient = regionToClient;
        this.regionToMaterializer = regionToMaterializer;
        this.snapshot = snapshot;
        this.requests = new MapMaker().makeMap();
    }
    
    @Override
    public ListenableFuture<Boolean> submit(
            final VolumeOperationDirective request) {
        VolumeOperationTask task = requests.get(request.getEntry());
        if (task == null) {
            final VolumeOperationTask newTask = new VolumeOperationTask(request);
            if (requests.putIfAbsent(request.getEntry(), newTask) != null) {
                return submit(request);
            }
            newTask.addListener(new Runnable() {
                @Override
                public void run() {
                    requests.remove(request.getEntry(), newTask);
                }
            }, MoreExecutors.directExecutor());
            task = LoggingFutureListener.listen(logger, newTask);
            task.run();
        } else {
            checkArgument(request.equals(task.task()));
        }
        return task;
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        for (Promise<?> future: Iterables.consumingIterable(requests.values())) {
            future.cancel(false);
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        for (Promise<?> future: Iterables.consumingIterable(requests.values())) {
            future.setException(failure);
        }
    }
    
    protected final class VolumeOperationTask extends PromiseTask<VolumeOperationDirective, Boolean> implements Runnable, ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureDequeChain<ListenableFuture<?>>> {

        private final ChainedFutures.ChainedFuturesTask<Boolean> chain;
        
        protected VolumeOperationTask(
                VolumeOperationDirective task) {
            super(task, SettableFuturePromise.<Boolean>create());
            this.chain = ChainedFutures.task(
                    ChainedFutures.result(
                            new Processor<FutureChain.FutureDequeChain<? extends ListenableFuture<?>>, Boolean>() {
                                @Override
                                public Boolean apply(
                                        FutureDequeChain<? extends ListenableFuture<?>> input)
                                        throws Exception {
                                    Iterator<? extends ListenableFuture<?>> previous = input.descendingIterator();
                                    while (previous.hasNext()) {
                                        previous.next().get();
                                    }
                                    return (Boolean) input.getLast().get();
                                }
                            },
                            ChainedFutures.arrayDeque(this)),
                    this);
        }
        
        @Override
        public void run() {
            chain.run();
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureDequeChain<ListenableFuture<?>> input) throws Exception {
            if (input.isEmpty()) {
                return Optional.of(Futures.immediateFuture(task().getOperation()));
            }
            if (input.getFirst() == input.getLast()) {
                ListenableFuture<?> future;
                try {
                    future = VolumeLookups.create(
                            task().getOperation(),
                            volumeToRegion,
                            volumeToPath);
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            if (input.getLast() instanceof VolumeLookups) {
                return Optional.of(RegionLookups.create(
                        this, 
                        (VolumeLookups) input.getLast(), 
                        regionToEnsemble,
                        regionToClient,
                        regionToMaterializer));
            } else if (input.getLast() instanceof RegionLookups) {
                Iterator<ListenableFuture<?>> previous = input.descendingIterator();
                RegionLookups<?> region = (RegionLookups<?>) previous.next();
                VolumeLookups volume = (VolumeLookups) previous.next();
                return Optional.of(
                    PrepareOperation.create(
                            task(),
                            volume,
                            region,
                            control));
            } else if (input.getLast() instanceof PrepareOperation) {
                try {
                    return snapshot.apply(input);
                } catch (Exception e) {
                    return Optional.of(CallSnapshot.create(
                            Futures.<Boolean>immediateFailedFuture(e)));
                }
            } else if (input.getLast() instanceof CallSnapshot) {
                Boolean isCommit;
                try { 
                    isCommit = (Boolean) input.getLast().get();
                } catch (Exception e) {
                    isCommit = Boolean.FALSE;
                }
                ListenableFuture<?> future;
                if (isCommit.booleanValue()) {
                    future = Futures.immediateFuture(isCommit);
                } else {
                    Iterator<ListenableFuture<?>> previous = input.iterator();
                    VolumeLookups volume = (VolumeLookups) Iterators.get(previous, 1);
                    RegionLookups<?> region = (RegionLookups<?>) previous.next();
                    future = UndoOperation.create(
                            task().getOperation(), 
                            volume,
                            region,
                            control);
                }
                return Optional.of(future);
            }
            return Optional.absent();
        }
    }
    
    public static final class InjectingSnapshotProcessor implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureDequeChain<ListenableFuture<?>>> {

        public static InjectingSnapshotProcessor create(
                Injector injector) {
            return new InjectingSnapshotProcessor(injector);
        }
        
        private final Injector injector;

        @Inject
        protected InjectingSnapshotProcessor(
                Injector injector) {
            this.injector = injector;
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureDequeChain<ListenableFuture<?>> input) throws Exception {
            Iterator<ListenableFuture<?>> itr = input.descendingIterator();
            Boolean isCommit = (Boolean) itr.next().get();
            RegionLookups<?> region = (RegionLookups<?>) itr.next();
            VolumeLookups volume = (VolumeLookups) itr.next();
            VolumeOperation<?> operation = (VolumeOperation<?>) itr.next().get();
            ImmutableMap.Builder<Identifier, ZNodePath> paths = 
                    ImmutableMap.builder();
            for (Map.Entry<Identifier, ListenableFuture<ZNodePath>> entry: volume.getPaths().entrySet()) {
                paths.put(entry.getKey(), entry.getValue().get());
            }
            Injector child = injector.createChildInjector(
                OperationModule.create(operation),
                SnapshotClientModule.create(
                        region.getFromEnsemble().get(), 
                        region.getFromClient().get().first(), 
                        region.getFromClient().get().second(), 
                        region.getToClient().get().second()),
                SnapshotVolumeModule.create(
                        operation, 
                        paths.build()),
                ExecuteSnapshot.module(),
                CoordinateSnapshot.module());
            AsyncFunction<Boolean, Boolean> snapshot = child.getInstance(
                    Key.get(new TypeLiteral<AsyncFunction<Boolean, Boolean>>(){}, 
                            edu.uw.zookeeper.safari.storage.snapshot.Snapshot.class));
            return Optional.of(CallSnapshot.create(isCommit, snapshot));
        }
    }
    
    public static final class ConnectedCallback<V extends AbstractConnectionClientExecutor<?,?,?,?,?>> extends SimpleToStringListenableFuture<V> implements Callable<Optional<V>>, Runnable {

        public static <V extends AbstractConnectionClientExecutor<?,?,?,?,?>> ListenableFuture<V> create(ListenableFuture<V> delegate) {
            ConnectedCallback<V> callback = new ConnectedCallback<V>(delegate);
            return callback.task;
        }
        
        private final CallablePromiseTask<?,V> task;
        
        protected ConnectedCallback(ListenableFuture<V> delegate) {
            super(delegate);
            this.task = CallablePromiseTask.create(this, SettableFuturePromise.<V>create());
            addListener(this, MoreExecutors.directExecutor());
        }

        @Override
        public Optional<V> call() throws Exception {
            if (isDone()) {
                V value = get();
                if (value.session().isDone()) {
                    if (value.session().get() instanceof ConnectMessage.Response.Valid) {
                        return Optional.of(value);
                    } else {
                        throw new KeeperException.SessionExpiredException();
                    }
                } else {
                    value.session().addListener(this, MoreExecutors.directExecutor());
                }
            }
            return Optional.absent();
        }

        @Override
        public void run() {
            task.run();
        }
    }
    
    public static final class EnsembleToLeader implements  AsyncFunction<EnsembleView<ServerInetAddressView>,ServerInetAddressView> {
        
        public static EnsembleToLeader create(
                ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
            return new EnsembleToLeader(anonymous);
        }
        
        private final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous;
        
        protected EnsembleToLeader(
                ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
            this.anonymous = anonymous;
        }
        
        @Override
        public ListenableFuture<ServerInetAddressView> apply(
                final EnsembleView<ServerInetAddressView> ensemble)
                throws Exception {
            return Futures.transform(
                    QueryZKLeader.call(ensemble, anonymous),
                    new Callback(ensemble));
        }
        
        protected final class Callback implements AsyncFunction<Optional<ServerInetAddressView>,ServerInetAddressView> {
            private final EnsembleView<ServerInetAddressView> ensemble;
            
            public Callback(EnsembleView<ServerInetAddressView> ensemble) {
                this.ensemble = ensemble;
            }
            
            @Override
            public ListenableFuture<ServerInetAddressView> apply(
                    Optional<ServerInetAddressView> input)
                    throws Exception {
                if (input.isPresent()) {
                    return Futures.immediateFuture(input.get());
                } else {
                    return EnsembleToLeader.this.apply(ensemble);
                }
            }
        }
    }
    
    public static final class PeerToStorageAddress implements AsyncFunction<Identifier, ServerInetAddressView> {

        public static PeerToStorageAddress create(
                Materializer<ControlZNode<?>,?> materializer) {
            return new PeerToStorageAddress(materializer);
        }

        private final Materializer<ControlZNode<?>,?> materializer;
        private final CachedPeerStorageAddress cached;
        
        protected PeerToStorageAddress(
                Materializer<ControlZNode<?>,?> materializer) {
            this.materializer = materializer;
            this.cached = CachedPeerStorageAddress.create(materializer.cache());
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public ListenableFuture<ServerInetAddressView> apply(
                Identifier peer) throws Exception {
            final AbsoluteZNodePath path = cached.pathOf(peer);
            Optional<ServerInetAddressView> cached = this.cached.apply(path);
            if (cached.isPresent()) {
                return Futures.immediateFuture(cached.get());
            } else {
                return Futures.transform(
                        SubmittedRequests.submitRequests(
                            materializer, 
                            Operations.Requests.sync().setPath(path).build(), 
                            Operations.Requests.getData().setPath(path).build()),
                        new Callback(path));
            }
        }

        protected final class Callback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, ServerInetAddressView> {

            private final AbsoluteZNodePath path;
            
            public Callback(AbsoluteZNodePath path) {
                this.path = path;
            }
            
            @Override
            public ListenableFuture<ServerInetAddressView> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    Operations.unlessError(response.record());
                }
                return Futures.immediateFuture(cached.apply(path).get());
            }
        }

        public static final class CachedPeerStorageAddress implements Function<AbsoluteZNodePath, Optional<ServerInetAddressView>> {

            public static CachedPeerStorageAddress create(
                    LockableZNodeCache<ControlZNode<?>,?,?> cache) {
                return new CachedPeerStorageAddress(cache);
            }
            
            private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
            
            protected CachedPeerStorageAddress(
                    LockableZNodeCache<ControlZNode<?>,?,?> cache) {
                this.cache = cache;
            }
            
            public AbsoluteZNodePath pathOf(Identifier peer) {
                return ControlSchema.Safari.Peers.Peer.StorageAddress.pathOf(peer);
            }

            @Override
            public Optional<ServerInetAddressView> apply(AbsoluteZNodePath input) {
                cache.lock().readLock().lock();
                try {
                    ControlZNode<?> node = cache.cache().get(input);
                    if ((node != null) && (node.data().stamp() > 0L)) {
                        return Optional.of((ServerInetAddressView) node.data().get());
                    }
                } finally {
                    cache.lock().readLock().unlock();
                }
                return Optional.absent();
            }
        }
    }
    
    public static final class RegionToEnsemble implements AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> {

        public static RegionToEnsemble create(
                Materializer<ControlZNode<?>,?> materializer) {
            return new RegionToEnsemble(materializer);
        }

        private final Materializer<ControlZNode<?>,?> materializer;
        private final CachedRegionMembers cached;
        private final PeerToStorageAddress addresses;
        private final AsyncFunction<List<Identifier>,EnsembleView<ServerInetAddressView>> toEnsemble;
        
        protected RegionToEnsemble(
                Materializer<ControlZNode<?>,?> materializer) {
            this.materializer = materializer;
            this.cached = CachedRegionMembers.create(materializer.cache());
            this.addresses = PeerToStorageAddress.create(materializer);
            final Function<List<ServerInetAddressView>,EnsembleView<ServerInetAddressView>> toEnsemble = new Function<List<ServerInetAddressView>,EnsembleView<ServerInetAddressView>>() {
                @Override
                public EnsembleView<ServerInetAddressView> apply(
                        List<ServerInetAddressView> input) {
                    return EnsembleView.copyOf(input);
                }
            };
            this.toEnsemble = new AsyncFunction<List<Identifier>,EnsembleView<ServerInetAddressView>>() {
                @Override
                public ListenableFuture<EnsembleView<ServerInetAddressView>> apply(
                        List<Identifier> members) throws Exception {
                    checkState(!members.isEmpty());
                    ImmutableList.Builder<ListenableFuture<ServerInetAddressView>> futures = ImmutableList.builder();
                    for (Identifier member: members) {
                        futures.add(addresses.apply(member));
                    }
                    return Futures.transform(
                            Futures.allAsList(futures.build()),
                            toEnsemble);
                }
            };
        }
        
        @Override
        public ListenableFuture<EnsembleView<ServerInetAddressView>> apply(
                Identifier region) throws Exception {
            final AbsoluteZNodePath path = cached.pathOf(region);
            List<Identifier> members = this.cached.apply(path);
            if (members.isEmpty()) {
                return Futures.transform(
                        SubmittedRequests.submitRequests(
                            materializer, 
                            Operations.Requests.sync().setPath(path).build(), 
                            Operations.Requests.getChildren().setPath(path).build()),
                        new Callback(path));
            } else {
                return toEnsemble.apply(members);
            }
        }

        protected final class Callback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, EnsembleView<ServerInetAddressView>> {

            private final AbsoluteZNodePath path;
            
            public Callback(AbsoluteZNodePath path) {
                this.path = path;
            }
            
            @Override
            public ListenableFuture<EnsembleView<ServerInetAddressView>> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    Operations.unlessError(response.record());
                }
                return toEnsemble.apply(RegionToEnsemble.this.cached.apply(path));
            }
        }
        
        public static final class CachedRegionMembers implements Function<AbsoluteZNodePath, List<Identifier>> {

            public static CachedRegionMembers create(
                    LockableZNodeCache<ControlZNode<?>,?,?> cache) {
                return new CachedRegionMembers(cache);
            }
            
            private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
            
            protected CachedRegionMembers(
                    LockableZNodeCache<ControlZNode<?>,?,?> cache) {
                this.cache = cache;
            }
            
            public AbsoluteZNodePath pathOf(Identifier region) {
                return ControlSchema.Safari.Regions.Region.Members.pathOf(region);
            }

            @Override
            public List<Identifier> apply(AbsoluteZNodePath input) {
                cache.lock().readLock().lock();
                try {
                    ControlZNode<?> node = cache.cache().get(input);
                    if (node != null) {
                        ImmutableList.Builder<Identifier> members = ImmutableList.builder();
                        for (ControlZNode<?> child: node.values()) {
                            members.add(((ControlSchema.Safari.Regions.Region.Members.Member) child).name());
                        }
                        return members.build();
                    }
                } finally {
                    cache.lock().readLock().unlock();
                }
                return ImmutableList.of();
            }
        }
    }
    
    public static final class VolumeToRegion implements AsyncFunction<VersionedId, Identifier> {
        
        public static VolumeToRegion create(
                Materializer<ControlZNode<?>,?> materializer) {
            return new VolumeToRegion(materializer);
        }
        
        private final Materializer<ControlZNode<?>,?> materializer;
        private final CachedVolumeToRegion cached;
        
        protected VolumeToRegion(
                Materializer<ControlZNode<?>,?> materializer) {
            this.materializer = materializer;
            this.cached = CachedVolumeToRegion.create(materializer.cache());
        }

        @SuppressWarnings("unchecked")
        @Override
        public ListenableFuture<Identifier> apply(final VersionedId input)
                throws Exception {
            final AbsoluteZNodePath path = cached.pathOf(input);
            Optional<Identifier> value = cached.apply(path);
            if (value.isPresent()) {
                return Futures.immediateFuture(value.get());
            } 
            return Futures.transform(
                        SubmittedRequests.submitRequests(
                            materializer, 
                            Operations.Requests.sync().setPath(path).build(), 
                            Operations.Requests.getData().setPath(path).build()),
                        new Callback(path));
        }
        
        protected final class Callback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>,Identifier> {

            private final AbsoluteZNodePath path;
            
            public Callback(AbsoluteZNodePath path) {
                this.path = path;
            }
            
            @Override
            public ListenableFuture<Identifier> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    Operations.unlessError(response.record());
                }
                return Futures.immediateFuture(cached.apply(path).get());
            }
        }
        
        public static final class CachedVolumeToRegion implements Function<AbsoluteZNodePath, Optional<Identifier>> {

            public static CachedVolumeToRegion create(
                    LockableZNodeCache<ControlZNode<?>,?,?> cache) {
                return new CachedVolumeToRegion(cache);
            }
            
            private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
            
            protected CachedVolumeToRegion(
                    LockableZNodeCache<ControlZNode<?>,?,?> cache) {
                this.cache = cache;
            }
            
            public AbsoluteZNodePath pathOf(VersionedId volume) {
                return ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(volume.getValue(), volume.getVersion());
            }

            @Override
            public Optional<Identifier> apply(AbsoluteZNodePath input) {
                cache.lock().readLock().lock();
                try {
                    ControlZNode<?> node = cache.cache().get(input);
                    if ((node != null) && (node.data().stamp() > 0L)) {
                        return Optional.of(((RegionAndLeaves) node.data().get()).getRegion());
                    }
                } finally {
                    cache.lock().readLock().unlock();
                }
                return Optional.absent();
            }
        }
    }
    
    protected abstract static class OperationStep<V> extends ToStringListenableFuture<V> implements Runnable {

        protected final PromiseTask<VolumeOperationDirective, Boolean> request;
        protected boolean hasRun;
        
        protected OperationStep(
                PromiseTask<VolumeOperationDirective, Boolean> request) {
            this.request = request;
            this.hasRun = false;
        }

        @Override
        public synchronized void run() {
            if (request.isDone()) {
                if (!isDone()) {
                    cancel(false);
                }
            } else if (isDone()) {
                if (!request.isDone() && !hasRun) {
                    hasRun = true;
                    try {
                        doRun();
                    } catch (Exception e) {
                        request.setException(e);
                    }
                }
            } else {
                request.addListener(this, MoreExecutors.directExecutor());
                addListener(this, MoreExecutors.directExecutor());
            }
        }
        
        protected abstract void doRun() throws Exception;
    }
    
    protected static final class VolumeLookups extends SimpleToStringListenableFuture<List<Object>> {
        
        public static VolumeLookups create(
                VolumeOperation<?> operation,
                AsyncFunction<VersionedId, Identifier> volumeToRegion,
                AsyncFunction<Identifier, ZNodePath> volumeToPath) throws Exception {
            ListenableFuture<Identifier> fromRegion = volumeToRegion.apply(operation.getVolume());
            ImmutableMap.Builder<Identifier, ListenableFuture<ZNodePath>> paths = 
                    ImmutableMap.<Identifier, ListenableFuture<ZNodePath>>builder()
                    .put(operation.getVolume().getValue(), volumeToPath.apply(operation.getVolume().getValue()));
            ListenableFuture<Identifier> toRegion;
            switch (operation.getOperator().getOperator()) {
            case MERGE:
            {
                VersionedId parent = ((MergeParameters) operation.getOperator().getParameters()).getParent();
                paths.put(parent.getValue(), volumeToPath.apply(parent.getValue()));
                toRegion = volumeToRegion.apply(parent);
                break;
            }
            case SPLIT:
            case TRANSFER:
            {
                toRegion = Futures.immediateFuture(((AssignParameters) operation.getOperator().getParameters()).getRegion());
                break;
            }
            default:
                throw new AssertionError();
            }
            return new VolumeLookups(fromRegion, toRegion, paths.build());
        }
        
        private final ListenableFuture<Identifier> fromRegion;
        private final ListenableFuture<Identifier> toRegion;
        private final ImmutableMap<Identifier, ListenableFuture<ZNodePath>> paths;
        
        protected VolumeLookups(
                ListenableFuture<Identifier> fromRegion,
                ListenableFuture<Identifier> toRegion,
                ImmutableMap<Identifier, ListenableFuture<ZNodePath>> paths) {
            super(Futures.allAsList(
                    ImmutableList.<ListenableFuture<?>>builder()
                        .add(fromRegion)
                        .add(toRegion)
                        .add(Futures.allAsList(paths.values()))
                        .build()));
            this.fromRegion = fromRegion;
            this.toRegion = toRegion;
            this.paths = paths;
        }
        
        public ListenableFuture<Identifier> getFromRegion() {
            return fromRegion;
        }
        
        public ListenableFuture<Identifier> getToRegion() {
            return toRegion;
        }
        
        public ImmutableMap<Identifier,ListenableFuture<ZNodePath>> getPaths() {
            return paths;
        }
    }
    
    protected static final class RegionLookups<T extends ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>,SessionListener,?>> extends SimpleToStringListenableFuture<List<Object>> {

        public static <T extends ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>,SessionListener,?>> RegionLookups<T> create(
                ListenableFuture<?> future,
                VolumeLookups volumes,
                AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> regionToEnsemble,
                AsyncFunction<Identifier, ? extends Pair<ServerInetAddressView, ? extends T>> regionToClient,
                AsyncFunction<Identifier, ? extends Materializer<StorageZNode<?>,?>> regionToMaterializer) throws Exception {
            final Identifier fromRegion = volumes.getFromRegion().get();
            final Identifier toRegion = volumes.getToRegion().get();
            final DisconnectTransform<T> disconnect = new DisconnectTransform<T>(future);
            ListenableFuture<EnsembleView<ServerInetAddressView>> fromEnsemble = regionToEnsemble.apply(fromRegion);
            ListenableFuture<? extends Materializer<StorageZNode<?>,?>> fromMaterializer = regionToMaterializer.apply(fromRegion);
            ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> fromClient = Futures.transform(regionToClient.apply(fromRegion), disconnect);
            ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> toClient = fromRegion.equals(toRegion) ? fromClient : Futures.transform(regionToClient.apply(toRegion), disconnect);
            return new RegionLookups<T>(fromEnsemble, fromMaterializer, fromClient, toClient);
        }
        
        private final ListenableFuture<EnsembleView<ServerInetAddressView>> fromEnsemble;
        private final ListenableFuture<? extends Materializer<StorageZNode<?>,?>> fromMaterializer;
        private final ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> fromClient;
        private final ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> toClient;
        
        protected RegionLookups(
                ListenableFuture<EnsembleView<ServerInetAddressView>> fromEnsemble,
                ListenableFuture<? extends Materializer<StorageZNode<?>,?>> fromMaterializer,
                ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> fromClient,
                ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> toClient) {
            super(Futures.allAsList(
                    ImmutableList.<ListenableFuture<?>>of(
                            fromEnsemble, 
                            fromMaterializer, 
                            fromClient, 
                            toClient)));
            this.fromEnsemble = fromEnsemble;
            this.fromClient = fromClient;
            this.fromMaterializer = fromMaterializer;
            this.toClient = toClient;
        }
        
        public ListenableFuture<EnsembleView<ServerInetAddressView>> getFromEnsemble() {
            return fromEnsemble;
        }

        public ListenableFuture<? extends Materializer<StorageZNode<?>, ?>> getFromMaterializer() {
            return fromMaterializer;
        }

        public ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> getFromClient() {
            return fromClient;
        }

        public ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> getToClient() {
            return toClient;
        }

        protected static final class DisconnectTransform<T extends ConnectionClientExecutor<?,?,?,?>> implements Function<Pair<ServerInetAddressView, ? extends T>, Pair<ServerInetAddressView, ? extends T>> {

            private final ListenableFuture<?> future;
            
            protected DisconnectTransform(
                    ListenableFuture<?> future) {
                this.future = future;
            }

            @Override
            public Pair<ServerInetAddressView, ? extends T> apply(
                    Pair<ServerInetAddressView, ? extends T> input) {
                new DisconnectWhenDone(future, input.second()).run();
                return input;
            }
        }
        
        protected static final class DisconnectWhenDone extends AbstractPair<ListenableFuture<?>,ConnectionClientExecutor<?,?,?,?>> implements Runnable {

            protected DisconnectWhenDone(
                    ListenableFuture<?> future,
                    ConnectionClientExecutor<?, ?, ?, ?> connection) {
                super(future, connection);
            }

            @Override
            public void run() {
                if (first.isDone()) {
                    AbstractConnectionClientExecutor.Disconnect.create(second);
                } else {
                    first.addListener(this, MoreExecutors.directExecutor());
                }
            }
        }
    }

    protected static final class PrepareOperation extends SimpleToStringListenableFuture<Boolean> {
        
        public static PrepareOperation create(
                final VolumeOperationDirective directive,
                final VolumeLookups volume,
                final RegionLookups<?> region,
                final Materializer<ControlZNode<?>,?> control) throws Exception {
            ListenableFuture<Boolean> future;
            if (directive.isCommit()) {
                Identifier fromVolume = directive.getOperation().getVolume().getValue();
                Identifier toVolume;
                switch (directive.getOperation().getOperator().getOperator()) {
                case SPLIT:
                    toVolume = ((SplitParameters) directive.getOperation().getOperator().getParameters()).getLeaf();
                    break;
                case MERGE:
                    toVolume = ((MergeParameters) directive.getOperation().getOperator().getParameters()).getParent().getValue();
                    break;
                case TRANSFER:
                    toVolume = directive.getOperation().getVolume().getValue();
                    break;
                default:
                    throw new AssertionError();
                }
                UnsignedLong version = directive.getOperation().getOperator().getParameters().getVersion();
                Callable<? extends ListenableFuture<Boolean>> callable = CreateToVersion.create(toVolume, version, region.getToClient().get().second());
                if (directive.getOperation().getOperator().getOperator() == VolumeOperator.SPLIT) {
                    callable = CreateFromVersion.create(fromVolume, version, region.getFromMaterializer().get(), callable);
                    callable = CreateLeafVolume.create(volume.getPaths().get(fromVolume).get(), (SplitParameters) directive.getOperation().getOperator().getParameters(), control, callable);
                }
                try {
                    future = callable.call();
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
            } else {
                future = Futures.immediateFuture(Boolean.valueOf(directive.isCommit()));
            }
            return new PrepareOperation(future);
        }
        
        protected PrepareOperation(
                ListenableFuture<Boolean> future) {
            super(future);
        }
        
        protected static final class CreateLeafVolume implements Callable<ListenableFuture<Boolean>>, AsyncFunction<Operation.ProtocolResponse<?>, Boolean> {

            public static CreateLeafVolume create(
                    ZNodePath root,
                    SplitParameters parameters,
                    Materializer<ControlZNode<?>,?> client,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                final Identifier leaf = parameters.getLeaf();
                final ZNodePath path = root.join(parameters.getBranch());
                return new CreateLeafVolume(
                        new IMultiRequest(
                                VolumesSchemaRequests.create(client)
                                .volume(leaf)
                                .create(path)),
                        client,
                        next);
            }
            
            private final Callable<? extends ListenableFuture<Boolean>> next;
            private final ClientExecutor<? super Records.Request,?,?> client;
            private final IMultiRequest request;
            
            protected CreateLeafVolume(
                    IMultiRequest request,
                    ClientExecutor<? super Records.Request,?,?> client,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                this.request = request;
                this.client = client;
                this.next = next;
            }
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequest.submit(
                                client, 
                                request),
                        this);
            }
            
            @Override
            public ListenableFuture<Boolean> apply(
                    Operation.ProtocolResponse<?> input) throws Exception {
                Operations.maybeMultiError((IMultiResponse) input.record(), KeeperException.Code.NODEEXISTS);
                return next.call();
            }
        }
        
        protected static final class CreateFromVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<Operation.ProtocolResponse<?>, Boolean> {

            public static CreateFromVersion create(
                    Identifier fromVolume,
                    UnsignedLong fromVersion,
                    Materializer<StorageZNode<?>,?> fromMaterializer,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                Callable<? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> operation = fromMaterializer.create(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.pathOf(fromVolume, fromVersion));
                return new CreateFromVersion(operation, next);
            }

            private final Callable<? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> operation;
            private final Callable<? extends ListenableFuture<Boolean>> next;
            
            protected CreateFromVersion(
                    Callable<? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> operation,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                this.operation = operation;
                this.next = next;
            }
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        operation.call(),
                        this);
            }
            
            @Override
            public ListenableFuture<Boolean> apply(
                    Operation.ProtocolResponse<?> input) throws Exception {
                Operations.maybeError(input.record(), KeeperException.Code.NODEEXISTS);
                return next.call();
            }
        }
        
        protected static final class CreateToVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            public static CreateToVersion create(
                    Identifier toVolume,
                    UnsignedLong toVersion,
                    ClientExecutor<? super Records.Request,?,?> toClient) {
                ZNodePath path = StorageSchema.Safari.Volumes.Volume.pathOf(toVolume);
                ImmutableList.Builder<ZNodePath> paths = ImmutableList.builder();
                paths.add(path);
                paths.add(path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL));
                path = path.join(StorageSchema.Safari.Volumes.Volume.Log.LABEL);
                paths.add(path);
                path = path.join(ZNodeLabel.fromString(toVersion.toString()));
                paths.add(path);
                Operations.Requests.Create create = Operations.Requests.create();
                ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
                for (ZNodePath p: paths.build()) {
                    requests.add(create.setPath(p).build());
                }
                return new CreateToVersion(requests.build(), toClient);
            }
            
            private final Iterable<? extends Records.Request> requests;
            private final ClientExecutor<? super Records.Request,?,?> client;
            
            public CreateToVersion(
                    Iterable<? extends Records.Request> requests,
                    ClientExecutor<? super Records.Request,?,?> client) {
                this.requests = requests;
                this.client = client;
            }
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequests.submit(
                                client, 
                                requests), 
                        this);
            }
            
            @Override
            public ListenableFuture<Boolean> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS);
                }
                return Futures.immediateFuture(Boolean.TRUE);
            }
        }
    }
    
    protected static final class CallSnapshot extends SimpleToStringListenableFuture<Boolean> {

        public static CallSnapshot create(
                Boolean isCommit,
                AsyncFunction<Boolean, Boolean> snapshot) {
            ListenableFuture<Boolean> future;
            try {
                future = snapshot.apply(isCommit);
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return create(future);
        }
        
        protected static CallSnapshot create(
                ListenableFuture<Boolean> future) {
            return new CallSnapshot(future);
        }
        
        protected CallSnapshot(
                ListenableFuture<Boolean> future) {
            super(future);
        }
    }

    // TODO: combine with PrepareOperation
    protected static final class UndoOperation extends SimpleToStringListenableFuture<Boolean> {
        
        public static UndoOperation create(
                final VolumeOperation<?> operation,
                final VolumeLookups volume,
                final RegionLookups<?> region,
                final Materializer<ControlZNode<?>, ?> control) throws Exception {
            Identifier fromVolume = operation.getVolume().getValue();
            UnsignedLong version = operation.getOperator().getParameters().getVersion();
            Identifier toVolume;
            switch (operation.getOperator().getOperator()) {
            case SPLIT:
                toVolume = ((SplitParameters) operation.getOperator().getParameters()).getLeaf();
                break;
            case MERGE:
                toVolume = ((MergeParameters) operation.getOperator().getParameters()).getParent().getValue();
                break;
            case TRANSFER:
                toVolume = fromVolume;
                break;
            default:
                throw new AssertionError();
            }
            Callable<ListenableFuture<Boolean>> callable = DeleteToVersion.create(toVolume, version, region.getToClient().get().second());
            if (operation.getOperator().getOperator() == VolumeOperator.SPLIT) {
                callable = DeleteFromVersion.create(fromVolume, version, region.getFromClient().get().second(), callable);
                callable = DeleteLeafVolume.create(
                        volume.getPaths().get(fromVolume).get(), 
                        (SplitParameters) operation.getOperator().getParameters(), 
                        control,
                        callable);
            }
            ListenableFuture<Boolean> future;
            try {
                future = callable.call();
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return new UndoOperation(future);
        }
        
        protected UndoOperation(
                ListenableFuture<Boolean> future) {
            super(future);
        }
        
        protected static final class DeleteLeafVolume implements Callable<ListenableFuture<Boolean>>, AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            public static DeleteLeafVolume create(
                    ZNodePath root,
                    SplitParameters parameters,
                    Materializer<ControlZNode<?>,?> control,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                final Identifier leaf = parameters.getLeaf();
                VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumePathSchemaRequests schema = (VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumePathSchemaRequests) VolumesSchemaRequests.create(control).volume(leaf).path();
                return new DeleteLeafVolume(root.join(parameters.getBranch()), schema, next);
            }
            
            private final ZNodePath root;
            private final VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumePathSchemaRequests schema;
            private final Callable<? extends ListenableFuture<Boolean>> next;
            
            protected DeleteLeafVolume(
                    ZNodePath root,
                    VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumePathSchemaRequests schema,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                this.root = root;
                this.schema = schema;
                this.next = next;
            }
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequests.submit(
                                schema.getVolume().volumes().getMaterializer(), 
                                schema.get()),
                        this);
            }

            @Override
            public ListenableFuture<Boolean> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                if (input.get(0).record() instanceof IMultiResponse) {
                    Operations.unlessMultiError((IMultiResponse) Iterables.getOnlyElement(input).record());
                } else {
                    Optional<Operation.Error> error = null;
                    for (Operation.ProtocolResponse<?> response: input) {
                        error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                    }
                    if (!error.isPresent()) {
                        final Materializer<ControlZNode<?>,?> control = schema.getVolume().volumes().getMaterializer();
                        control.cache().lock().readLock().lock();
                        try {
                            ControlSchema.Safari.Volumes.Volume.Path node = (ControlSchema.Safari.Volumes.Volume.Path) control.cache().cache().get(schema.getPath());
                            if (root.equals(node.data().get())) {
                                return Futures.transform(
                                        SubmittedRequests.submitRequests(
                                                control, 
                                                new IMultiRequest(
                                                        schema.getVolume().delete())),
                                        this);
                            }
                        } finally {
                            control.cache().lock().readLock().unlock();
                        }
                    }
                }
                return next.call();
            }
        }
        
        protected static final class DeleteFromVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<Operation.ProtocolResponse<?>, Boolean> {

            public static DeleteFromVersion create(
                    Identifier fromVolume,
                    UnsignedLong fromVersion,
                    ClientExecutor<? super Records.Request,?,?> fromClient,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                return new DeleteFromVersion(fromVolume, fromVersion, fromClient, next);
            }

            private final Identifier volume;
            private final UnsignedLong version;
            private final ClientExecutor<? super Records.Request,?,?> client;
            private final Callable<? extends ListenableFuture<Boolean>> next;
            
            protected DeleteFromVersion(
                    Identifier volume,
                    UnsignedLong version,
                    ClientExecutor<? super Records.Request,?,?> client,
                    Callable<? extends ListenableFuture<Boolean>> next) {
                this.volume = volume;
                this.version = version;
                this.client = client;
                this.next = next;
            }
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequest.submit(
                            client, 
                            Operations.Requests.delete().setPath(
                                    StorageSchema.Safari.Volumes.Volume.Log.Version.pathOf(volume, version))
                                    .build()),
                        this);
            }

            @Override
            public ListenableFuture<Boolean> apply(
                    Operation.ProtocolResponse<?> input) throws Exception {
                Operations.maybeError(input.record(), KeeperException.Code.NONODE);
                return next.call();
            }
        }
        
        protected static final class DeleteToVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            public static DeleteToVersion create(
                    Identifier toVolume,
                    UnsignedLong toVersion,
                    ClientExecutor<? super Records.Request,?,?> toClient) {
                return new DeleteToVersion(toVolume, toVersion, toClient);
            }
            
            private final Identifier volume;
            private final UnsignedLong version;
            private final ClientExecutor<? super Records.Request,?,?> client;
            
            protected DeleteToVersion(
                    Identifier volume,
                    UnsignedLong version,
                    ClientExecutor<? super Records.Request,?,?> client) {
                this.volume = volume;
                this.version = version;
                this.client = client;
            }
            
            @Override
            @SuppressWarnings("unchecked")
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequests.submit(
                                client,
                                PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getChildren())
                                    .apply(StorageSchema.Safari.Volumes.Volume.Log.pathOf(volume))), 
                        this);       
            }
            
            @Override
            public ListenableFuture<Boolean> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                if (input.get(0).record() instanceof IMultiResponse) {
                    Operations.unlessMultiError((IMultiResponse) Iterables.getOnlyElement(input).record());
                } else {
                    for (Operation.ProtocolResponse<?> response: input) {
                        Optional<Operation.Error> error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                        if (!error.isPresent() && (response.record() instanceof Records.ChildrenGetter)) {
                            final List<String> children = ((Records.ChildrenGetter) response.record()).getChildren();
                            String version = this.version.toString();
                            if (children.contains(version)) {
                                final boolean onlyVersion = children.size() == 1;
                                ZNodePath path = StorageSchema.Safari.Volumes.Volume.pathOf(volume);
                                ImmutableList.Builder<ZNodePath> paths = ImmutableList.builder();
                                if (onlyVersion) {
                                    paths.add(path);
                                    paths.add(path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL));
                                }
                                path = path.join(StorageSchema.Safari.Volumes.Volume.Log.LABEL);
                                if (onlyVersion) {
                                    paths.add(path);
                                }
                                path = path.join(ZNodeLabel.fromString(version));
                                paths.add(path);
                                Operations.Requests.Delete delete = Operations.Requests.delete();
                                ImmutableList.Builder<Records.MultiOpRequest> requests = ImmutableList.builder();
                                for (ZNodePath p: Lists.reverse(paths.build())) {
                                    requests.add(delete.setPath(p).build());
                                }
                                return Futures.transform(
                                        SubmittedRequests.submit(
                                                client, 
                                                new IMultiRequest(requests.build())), 
                                        this);
                            }
                        }         
                    }
                }
                return Futures.immediateFuture(Boolean.valueOf(Boolean.FALSE));
            }
        }
    }
}
