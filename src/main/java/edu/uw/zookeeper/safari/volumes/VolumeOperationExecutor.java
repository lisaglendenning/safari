package edu.uw.zookeeper.safari.volumes;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.QueryZKLeader;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.ServiceMonitor;
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
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.AbstractConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.AnonymousClientConnection;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.volumes.VolumeDescriptorCache;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationDirective;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.schema.volumes.AssignParameters;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.snapshot.CoordinateSnapshot;

public final class VolumeOperationExecutor<O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> extends LoggingServiceListener<VolumeOperationExecutor<O,T>> implements TaskExecutor<VolumeOperationDirective, Boolean> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        protected Module() {}

        @Provides
        public VolumeOperationExecutor<Message.ServerResponse<?>,OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> newVolumeExecutor(
                final @Control Materializer<ControlZNode<?>,?> control,
                final VolumeDescriptorCache descriptors,
                final @Region Identifier region,
                final @Storage Materializer<StorageZNode<?>,?> materializer,
                final @Storage ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> storage,
                final @Storage ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
                final @Storage TimeValue timeOut,
                final NetClientModule clientModule,
                final ServiceMonitor monitor,
                final ScheduledExecutorService scheduler) {
            final ClientConnectionFactory<? extends AnonymousClientConnection<?,?>> anonymous = AnonymousClientConnection.defaults(clientModule);
            monitor.add(anonymous);
            Services.startAndWait(anonymous);
            final RegionToEnsemble regionToEnsemble = RegionToEnsemble.create(
                    control);
            final EnsembleToLeader ensembleToLeader = EnsembleToLeader.create(anonymous);
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
            return VolumeOperationExecutor.create(
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
                    anonymous);
        }

        @Override
        protected void configure() {
            bind(new TypeLiteral<VolumeOperationExecutor<?,?>>(){}).to(new TypeLiteral<VolumeOperationExecutor<Message.ServerResponse<?>,OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>() {});
            bind(new TypeLiteral<TaskExecutor<VolumeOperationDirective, Boolean>>(){}).to(new TypeLiteral<VolumeOperationExecutor<?,?>>(){});
        }
    }
    
    public static <O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> VolumeOperationExecutor<O,T> create(
            Materializer<ControlZNode<?>,?> control,
            AsyncFunction<Identifier, ZNodePath> volumeToPath,
            AsyncFunction<VersionedId, Identifier> volumeToRegion,
            AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> regionToEnsemble,
            AsyncFunction<Identifier, ? extends Pair<ServerInetAddressView, ? extends T>> regionToClient,
            AsyncFunction<Identifier, ? extends Materializer<StorageZNode<?>,?>> regionToMaterializer,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
        return new VolumeOperationExecutor<O,T>(
                control,
                volumeToPath,
                volumeToRegion,
                regionToEnsemble,
                regionToClient,
                regionToMaterializer,
                anonymous);
    }
    
    private final Materializer<ControlZNode<?>,?> control;
    private final AsyncFunction<Identifier, ZNodePath> volumeToPath;
    private final AsyncFunction<VersionedId, Identifier> volumeToRegion;
    private final AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> regionToEnsemble;
    private final AsyncFunction<Identifier, ? extends Pair<ServerInetAddressView, ? extends T>> regionToClient;
    private final AsyncFunction<Identifier, ? extends Materializer<StorageZNode<?>,?>> regionToMaterializer;
    private final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous;
    
    protected VolumeOperationExecutor(
            Materializer<ControlZNode<?>,?> control,
            AsyncFunction<Identifier, ZNodePath> volumeToPath,
            AsyncFunction<VersionedId, Identifier> volumeToRegion,
            AsyncFunction<Identifier, EnsembleView<ServerInetAddressView>> regionToEnsemble,
            AsyncFunction<Identifier, ? extends Pair<ServerInetAddressView, ? extends T>> regionToClient,
            AsyncFunction<Identifier, ? extends Materializer<StorageZNode<?>,?>> regionToMaterializer,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
        this.control = control;
        this.volumeToPath = volumeToPath;
        this.volumeToRegion = volumeToRegion;
        this.regionToEnsemble = regionToEnsemble;
        this.regionToClient = regionToClient;
        this.regionToMaterializer = regionToMaterializer;
        this.anonymous = anonymous;
    }
    
    // TODO track submitted and cancel on termination
    @Override
    public ListenableFuture<Boolean> submit(VolumeOperationDirective request) {
        PromiseTask<VolumeOperationDirective, Boolean> task = PromiseTask.of(request);
        try {
            new VolumeLookups(task).run();
        } catch (Exception e) {
            throw new RejectedExecutionException(e);
        }
        return task;
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
    
    protected final class VolumeLookups extends OperationStep<List<Object>> {
        
        private final ListenableFuture<Identifier> fromRegion;
        private final ListenableFuture<Identifier> toRegion;
        private final ListenableFuture<List<ZNodePath>> paths;
        private final ListenableFuture<List<Object>> future;
        
        public VolumeLookups(
                PromiseTask<VolumeOperationDirective, Boolean> request) throws Exception {
            super(request);
            this.fromRegion = volumeToRegion.apply(request.task().getOperation().getVolume());
            ImmutableList.Builder<ListenableFuture<ZNodePath>> paths = 
                    ImmutableList.<ListenableFuture<ZNodePath>>builder()
                    .add(volumeToPath.apply(request.task().getOperation().getVolume().getValue()));
            switch (request.task().getOperation().getOperator().getOperator()) {
            case MERGE:
            {
                VersionedId parent = ((MergeParameters) request.task().getOperation().getOperator().getParameters()).getParent();
                paths.add(volumeToPath.apply(parent.getValue()));
                this.toRegion = volumeToRegion.apply(parent);
                break;
            }
            case SPLIT:
            case TRANSFER:
            {
                this.toRegion = Futures.immediateFuture(((AssignParameters) request.task().getOperation().getOperator().getParameters()).getRegion());
                break;
            }
            default:
                throw new AssertionError();
            }
            this.paths = Futures.allAsList(paths.build());
            this.future = Futures.successfulAsList(
                    ImmutableList.<ListenableFuture<?>>builder()
                        .add(fromRegion)
                        .add(toRegion)
                        .add(this.paths)
                        .build());
        }

        @Override
        protected void doRun() throws Exception {
            new RegionLookups(this.fromRegion.get(), this.toRegion.get(), paths.get(), request).run();
        }

        @Override
        protected ListenableFuture<List<Object>> delegate() {
            return future;
        }
    }
    
    protected final class RegionLookups extends OperationStep<List<Object>> implements Function<Pair<ServerInetAddressView, ? extends T>, Pair<ServerInetAddressView, ? extends T>> {

        private final List<ZNodePath> paths;
        private final ListenableFuture<EnsembleView<ServerInetAddressView>> fromEnsemble;
        private final ListenableFuture<? extends Materializer<StorageZNode<?>,?>> fromMaterializer;
        private final ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> fromClient;
        private final ListenableFuture<? extends Pair<ServerInetAddressView, ? extends T>> toClient;
        private final ListenableFuture<List<Object>> future;
        
        public RegionLookups(
                Identifier fromRegion, 
                Identifier toRegion, 
                List<ZNodePath> paths,
                PromiseTask<VolumeOperationDirective, Boolean> request) throws Exception {
            super(request);
            this.paths = paths;
            this.fromEnsemble = regionToEnsemble.apply(fromRegion);
            this.fromMaterializer = regionToMaterializer.apply(fromRegion);
            this.fromClient = Futures.transform(regionToClient.apply(fromRegion), this);
            this.toClient = fromRegion.equals(toRegion) ? fromClient : Futures.transform(regionToClient.apply(toRegion), this);
            this.future = Futures.successfulAsList(
                    ImmutableList.<ListenableFuture<?>>of(
                            fromEnsemble, fromMaterializer, fromClient, toClient));
        }

        @Override
        public Pair<ServerInetAddressView, ? extends T> apply(
                Pair<ServerInetAddressView, ? extends T> input) {
            new DisconnectWhenDone(input.second()).run();
            return input;
        }

        @Override
        protected void doRun() throws Exception {
            new PrepareOperation(
                    fromEnsemble.get(), 
                    fromMaterializer.get(), 
                    fromClient.get(), 
                    toClient.get(), 
                    paths, 
                    request).run();
        }

        @Override
        protected ListenableFuture<List<Object>> delegate() {
            return future;
        }
        
        protected class DisconnectWhenDone extends AbstractPair<ListenableFuture<?>,ConnectionClientExecutor<?,?,?,?>> implements Runnable {

            protected DisconnectWhenDone(
                    ConnectionClientExecutor<?, ?, ?, ?> second) {
                super(request, second);
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

    protected final class PrepareOperation extends OperationStep<Boolean> {
        
        private final EnsembleView<ServerInetAddressView> fromEnsemble;
        private final Materializer<StorageZNode<?>,?> fromMaterializer;
        private final Pair<ServerInetAddressView, ? extends T> fromClient;
        private final Pair<ServerInetAddressView, ? extends T> toClient;
        private final List<ZNodePath> paths;
        private final ListenableFuture<Boolean> future;
        
        public PrepareOperation(
                final EnsembleView<ServerInetAddressView> fromEnsemble,
                final Materializer<StorageZNode<?>,?> fromMaterializer,
                final Pair<ServerInetAddressView, ? extends T> fromClient,
                final Pair<ServerInetAddressView, ? extends T> toClient,
                final List<ZNodePath> paths,
                final PromiseTask<VolumeOperationDirective, Boolean> request) {
            super(request);
            this.fromEnsemble = fromEnsemble;
            this.fromMaterializer = fromMaterializer;
            this.fromClient = fromClient;
            this.toClient = toClient;
            this.paths = paths;
            ListenableFuture<Boolean> future;
            if (request.task().isCommit()) {
                Callable<? extends ListenableFuture<Boolean>> callable;
                if (request.task().getOperation().getOperator().getOperator() == VolumeOperator.SPLIT) {
                    callable = new CreateLeafVolume();
                } else {
                    callable = new CreateToVersion();
                }
                try {
                    future = callable.call();
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
            } else {
                future = Futures.immediateFuture(Boolean.valueOf(request.task().isCommit()));
            }
            this.future = future;
        }

        @Override
        protected void doRun() throws Exception {
            new CallSnapshot(
                    future.get().booleanValue(),
                    fromEnsemble, 
                    fromMaterializer, 
                    fromClient, 
                    toClient, 
                    paths, 
                    request,
                    anonymous).run();
        }

        @Override
        protected ListenableFuture<Boolean> delegate() {
            return future;
        }
        
        protected class CreateLeafVolume implements Callable<ListenableFuture<Boolean>>, AsyncFunction<Operation.ProtocolResponse<?>, Boolean> {

            public CreateLeafVolume() {}
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                final SplitParameters parameters = (SplitParameters) request.task().getOperation().getOperator().getParameters();
                final Identifier leaf = parameters.getLeaf();
                final ZNodePath path = paths.get(0).join(parameters.getBranch());
                return Futures.transform(
                        SubmittedRequest.submit(
                                control, 
                                new IMultiRequest(
                                        VolumesSchemaRequests.create(control)
                                        .volume(leaf)
                                        .create(path))),
                        this);
            }
            
            @Override
            public ListenableFuture<Boolean> apply(
                    Operation.ProtocolResponse<?> input) throws Exception {
                Operations.maybeMultiError((IMultiResponse) input.record(), KeeperException.Code.NODEEXISTS);
                return new CreateFromVersion().call();
            }
        }
        
        protected class CreateFromVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<Operation.ProtocolResponse<?>, Boolean> {

            public CreateFromVersion() {}
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        fromMaterializer.create(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.pathOf(request.task().getOperation().getVolume().getValue(), request.task().getOperation().getOperator().getParameters().getVersion()))
                                .call(),
                        this);
            }
            
            @Override
            public ListenableFuture<Boolean> apply(
                    Operation.ProtocolResponse<?> input) throws Exception {
                Operations.maybeError(input.record(), KeeperException.Code.NODEEXISTS);
                return new CreateToVersion().call();
            }
        }
        
        protected class CreateToVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            private final Identifier toVolume;
            
            public CreateToVersion() {
                switch (request.task().getOperation().getOperator().getOperator()) {
                case SPLIT:
                    this.toVolume = ((SplitParameters) request.task().getOperation().getOperator().getParameters()).getLeaf();
                    break;
                case MERGE:
                    this.toVolume = ((MergeParameters) request.task().getOperation().getOperator().getParameters()).getParent().getValue();
                    break;
                case TRANSFER:
                    this.toVolume = request.task().getOperation().getVolume().getValue();
                    break;
                default:
                    throw new AssertionError();
                }
            }
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                ZNodePath path = StorageSchema.Safari.Volumes.Volume.pathOf(toVolume);
                ImmutableList.Builder<ZNodePath> paths = ImmutableList.builder();
                paths.add(path);
                paths.add(path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL));
                path = path.join(StorageSchema.Safari.Volumes.Volume.Log.LABEL);
                paths.add(path);
                path = path.join(ZNodeLabel.fromString(request.task().getOperation().getOperator().getParameters().getVersion().toString()));
                paths.add(path);
                Operations.Requests.Create create = Operations.Requests.create();
                ImmutableList.Builder<Records.Request> requests = ImmutableList.builder();
                for (ZNodePath p: paths.build()) {
                    requests.add(create.setPath(p).build());
                }
                return Futures.transform(
                        SubmittedRequests.submit(
                                toClient.second(), 
                                requests.build()), 
                        this);
            }
            
            @Override
            public ListenableFuture<Boolean> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS);
                }
                return Futures.immediateFuture(Boolean.valueOf(request.task().isCommit()));
            }
        }
    }
    
    protected final class CallSnapshot extends OperationStep<Boolean> {

        private final List<ZNodePath> paths;
        private final ListenableFuture<Boolean> future;
        private final T fromClient;
        private final T toClient;
        
        protected CallSnapshot(
                final boolean isCommit,
                final EnsembleView<ServerInetAddressView> fromEnsemble,
                final Materializer<StorageZNode<?>,?> fromMaterializer,
                final Pair<ServerInetAddressView, ? extends T> fromClient,
                final Pair<ServerInetAddressView, ? extends T> toClient,
                final List<ZNodePath> paths,
                final PromiseTask<VolumeOperationDirective, Boolean> request,
                final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
            super(request);
            this.paths = paths;
            this.fromClient = fromClient.second();
            this.toClient = toClient.second();
            this.future = CoordinateSnapshot.call(
                    isCommit,
                    request.task().getOperation(),
                    fromEnsemble, 
                    fromMaterializer, 
                    fromClient, 
                    toClient, 
                    paths,
                    anonymous);
        }

        @Override
        protected void doRun() throws Exception {
            if (get().booleanValue()) {
                request.set(Boolean.TRUE);
            } else {
                UndoOperation.create(control, paths, fromClient, toClient, request).run();
            }
        }

        @Override
        protected ListenableFuture<Boolean> delegate() {
            return future;
        }
    }

    protected static final class UndoOperation<O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> extends OperationStep<Boolean> {
        
        public static <O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> UndoOperation<O,T> create(
                final Materializer<ControlZNode<?>, ?> materializer,
                final List<ZNodePath> paths,
                final T fromClient,
                final T toClient,
                final PromiseTask<VolumeOperationDirective, Boolean> request) {
            return new UndoOperation<O,T>(materializer, paths, fromClient, toClient, request);
        }
        
        private final Materializer<ControlZNode<?>, ?> control;
        private final List<ZNodePath> paths;
        private final ListenableFuture<Boolean> future;
        private final T fromClient;
        private final T toClient;
        
        public UndoOperation(
                final Materializer<ControlZNode<?>, ?> control,
                final List<ZNodePath> paths,
                final T fromClient,
                final T toClient,
                final PromiseTask<VolumeOperationDirective, Boolean> request) {
            super(request);
            this.control = control;
            this.paths = paths;
            this.fromClient = fromClient;
            this.toClient = toClient;
            Callable<? extends ListenableFuture<Boolean>> callable;
            if (request.task().getOperation().getOperator().getOperator() == VolumeOperator.SPLIT) {
                callable = new DeleteLeafVolume();
            } else {
                callable = new DeleteToVersion();
            }
            ListenableFuture<Boolean> future;
            try {
                future = callable.call();
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            this.future = future;
        }

        @Override
        protected void doRun() throws Exception {
            assert (future != request);
            if (!isCancelled()) {
                request.set(future.get());
            }
        }

        @Override
        protected ListenableFuture<Boolean> delegate() {
            return future;
        }
        
        protected class DeleteLeafVolume implements Callable<ListenableFuture<Boolean>>, AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            private final VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumePathSchemaRequests schema;
            
            public DeleteLeafVolume() {
                final Identifier leaf = ((SplitParameters) request.task().getOperation().getOperator().getParameters()).getLeaf();
                this.schema = (VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumePathSchemaRequests) VolumesSchemaRequests.create(control).volume(leaf).path();
            }
            
            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequests.submit(
                                control, 
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
                        control.cache().lock().readLock().lock();
                        try {
                            ControlSchema.Safari.Volumes.Volume.Path node = (ControlSchema.Safari.Volumes.Volume.Path) control.cache().cache().get(schema.getPath());
                            if (paths.get(0).join(((SplitParameters) request.task().getOperation().getOperator().getParameters()).getBranch()).equals(node.data().get())) {
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
                return new DeleteFromVersion().call();
            }
        }
        
        protected class DeleteFromVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<Operation.ProtocolResponse<?>, Boolean> {

            @Override
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequest.submit(
                            fromClient, 
                            Operations.Requests.delete().setPath(
                                    StorageSchema.Safari.Volumes.Volume.Log.Version.pathOf(request.task().getOperation().getVolume().getValue(), request.task().getOperation().getOperator().getParameters().getVersion()))
                                    .build()),
                        this);
            }

            @Override
            public ListenableFuture<Boolean> apply(
                    Operation.ProtocolResponse<?> input) throws Exception {
                Operations.maybeError(input.record(), KeeperException.Code.NONODE);
                return new DeleteToVersion().call();
            }
        }
        
        protected class DeleteToVersion implements Callable<ListenableFuture<Boolean>>, AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            private final Identifier toVolume;
            
            public DeleteToVersion() {
                switch (request.task().getOperation().getOperator().getOperator()) {
                case SPLIT:
                    this.toVolume = ((SplitParameters) request.task().getOperation().getOperator().getParameters()).getLeaf();
                    break;
                case MERGE:
                    this.toVolume = ((MergeParameters) request.task().getOperation().getOperator().getParameters()).getParent().getValue();
                    break;
                case TRANSFER:
                    this.toVolume = request.task().getOperation().getVolume().getValue();
                    break;
                default:
                    throw new AssertionError();
                }
            }
            
            @Override
            @SuppressWarnings("unchecked")
            public ListenableFuture<Boolean> call() throws Exception {
                return Futures.transform(
                        SubmittedRequests.submit(
                                toClient,
                                PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getChildren())
                                    .apply(StorageSchema.Safari.Volumes.Volume.Log.pathOf(toVolume))), 
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
                            if (children.contains(request.task().getOperation().getOperator().getParameters().getVersion().toString())) {
                                final boolean onlyVersion = children.size() == 1;
                                ZNodePath path = StorageSchema.Safari.Volumes.Volume.pathOf(toVolume);
                                ImmutableList.Builder<ZNodePath> paths = ImmutableList.builder();
                                if (onlyVersion) {
                                    paths.add(path);
                                    paths.add(path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL));
                                }
                                path = path.join(StorageSchema.Safari.Volumes.Volume.Log.LABEL);
                                if (onlyVersion) {
                                    paths.add(path);
                                }
                                path = path.join(ZNodeLabel.fromString(request.task().getOperation().getOperator().getParameters().getVersion().toString()));
                                paths.add(path);
                                Operations.Requests.Delete delete = Operations.Requests.delete();
                                ImmutableList.Builder<Records.MultiOpRequest> requests = ImmutableList.builder();
                                for (ZNodePath p: Lists.reverse(paths.build())) {
                                    requests.add(delete.setPath(p).build());
                                }
                                return Futures.transform(
                                        SubmittedRequests.submit(
                                                toClient, 
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
