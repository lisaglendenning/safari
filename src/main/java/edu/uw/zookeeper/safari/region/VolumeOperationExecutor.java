package edu.uw.zookeeper.safari.region;

import static com.google.common.base.Preconditions.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.QueryZKLeader;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
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
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.data.VolumeDescriptorCache;
import edu.uw.zookeeper.safari.storage.Snapshot;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.volume.AssignParameters;
import edu.uw.zookeeper.safari.volume.MergeParameters;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;
import edu.uw.zookeeper.safari.volume.SplitParameters;
import edu.uw.zookeeper.safari.volume.VolumeOperator;

public final class VolumeOperationExecutor<O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> implements TaskExecutor<VolumeOperationDirective, Boolean> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}

        @Provides @Singleton
        public VolumeOperationExecutor<Message.ServerResponse<?>,OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> newVolumeExecutor(
                final @Control Materializer<ControlZNode<?>,?> control,
                final VolumeDescriptorCache idToPath,
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
                            storage.get(),
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
                                    connections.connect(server.get()), 
                                    new Function<ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>, Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>() {
                                        @Override
                                        public Pair<ServerInetAddressView, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>> apply(ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?> input) {
                                            return Pair.create(server, 
                                                    OperationClientExecutor.newInstance(
                                                    ConnectMessage.Request.NewRequest.newInstance(timeOut, 0L), 
                                                    input, 
                                                    scheduler));
                                        }
                                    }, SameThreadExecutor.getInstance());
                        }
                };
            return VolumeOperationExecutor.create(
                    control,
                    idToPath.asLookup(),
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
                                            ensembleToLeader,
                                            SameThreadExecutor.getInstance()),
                                        clients,
                                        SameThreadExecutor.getInstance());
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
            bind(new TypeLiteral<TaskExecutor<VolumeOperationDirective, Boolean>>(){}).to(new TypeLiteral<VolumeOperationExecutor<Message.ServerResponse<?>,OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response, ?, ?, ?>>>>() {});
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
                    new Callback(ensemble), SameThreadExecutor.getInstance());
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
                        new Callback(path),
                        SameThreadExecutor.getInstance());
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
                            toEnsemble,
                            SameThreadExecutor.getInstance());
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
                        new Callback(path),
                        SameThreadExecutor.getInstance());
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
                        new Callback(path),
                        SameThreadExecutor.getInstance());
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
                request.addListener(this, SameThreadExecutor.getInstance());
                addListener(this, SameThreadExecutor.getInstance());
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
    
    protected final class RegionLookups extends OperationStep<List<Object>> {

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
            this.fromClient = regionToClient.apply(fromRegion);
            this.toClient = fromRegion.equals(toRegion) ? fromClient : regionToClient.apply(toRegion);
            this.future = Futures.successfulAsList(
                    ImmutableList.<ListenableFuture<?>>of(
                            fromEnsemble, fromMaterializer, fromClient, toClient));
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
            if (request.task().isCommit() && (request.task().getOperation().getOperator().getOperator() == VolumeOperator.SPLIT)) {
                final SplitParameters parameters = (SplitParameters) request.task().getOperation().getOperator().getParameters();
                final Identifier leaf = parameters.getLeaf();
                final ZNodePath path = paths.get(0).join(parameters.getBranch());
                future = Futures.transform(
                        SubmittedRequests.submitRequests(
                                control, 
                                new IMultiRequest(VolumesSchemaRequests.create(control).volume(leaf).create(path))),
                        new ResponseCallback(),
                        SameThreadExecutor.getInstance());
            } else {
                future = Futures.immediateFuture(Boolean.valueOf(request.task().isCommit()));
            }
        }

        @Override
        protected void doRun() throws Exception {
            ExecuteSnapshot.execute(
                    future.get().booleanValue(),
                    fromEnsemble, 
                    fromMaterializer, 
                    fromClient, 
                    toClient, 
                    paths, 
                    request,
                    control,
                    anonymous);
        }

        @Override
        protected ListenableFuture<Boolean> delegate() {
            return future;
        }
        
        protected class ResponseCallback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            public ResponseCallback() {}
            
            @Override
            public ListenableFuture<Boolean> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    if (response.record() instanceof IMultiResponse) {
                        Operations.maybeMultiError((IMultiResponse) response.record(), KeeperException.Code.NODEEXISTS);
                    } else {
                        Operations.unlessError(response.record());
                    }
                }
                return Futures.immediateFuture(Boolean.valueOf(request.task().isCommit()));
            }
        }
    }

    protected static final class UndoOperation extends OperationStep<Boolean> {
        
        public static UndoOperation create(
                final Materializer<ControlZNode<?>, ?> materializer,
                final List<ZNodePath> paths,
                final PromiseTask<VolumeOperationDirective, Boolean> request) {
            return new UndoOperation(materializer, paths, request);
        }
        
        private final List<ZNodePath> paths;
        private final Materializer<ControlZNode<?>, ?> materializer;
        private final ListenableFuture<Boolean> future;
        
        @SuppressWarnings("unchecked")
        public UndoOperation(
                final Materializer<ControlZNode<?>, ?> materializer,
                final List<ZNodePath> paths,
                final PromiseTask<VolumeOperationDirective, Boolean> request) {
            super(request);
            this.materializer = materializer;
            this.paths = paths;
            if (request.task().getOperation().getOperator().getOperator() == VolumeOperator.SPLIT) {
                final SplitParameters parameters = (SplitParameters) request.task().getOperation().getOperator().getParameters();
                final Identifier leaf = parameters.getLeaf();
                final ZNodePath path = ControlSchema.Safari.Volumes.Volume.Path.pathOf(leaf);
                future = Futures.transform(
                        SubmittedRequests.submitRequests(
                                materializer, 
                                Operations.Requests.sync().setPath(path).build(),
                                Operations.Requests.getData().setPath(path).build()),
                        new ResponseCallback(),
                        SameThreadExecutor.getInstance());
            } else {
                future = Futures.immediateFuture(Boolean.FALSE);
            }
        }

        @Override
        protected void doRun() throws Exception {
            future.get();
        }

        @Override
        protected ListenableFuture<Boolean> delegate() {
            return future;
        }
        
        protected class ResponseCallback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean> {

            public ResponseCallback() {}
            
            @Override
            public ListenableFuture<Boolean> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                final SplitParameters parameters = (SplitParameters) request.task().getOperation().getOperator().getParameters();
                final Identifier leaf = parameters.getLeaf();
                Optional<Operation.Error> error = null;
                for (Operation.ProtocolResponse<?> response: input) {
                    error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                }
                if (!error.isPresent()) {
                    materializer.cache().lock().readLock().lock();
                    try {
                        ControlSchema.Safari.Volumes.Volume.Path node = (ControlSchema.Safari.Volumes.Volume.Path) materializer.cache().cache().get(ControlSchema.Safari.Volumes.Volume.Path.pathOf(leaf));
                        if (paths.get(0).join(parameters.getBranch()).equals(node.data().get())) {
                            return Futures.transform(
                                    SubmittedRequests.submitRequests(materializer, 
                                            new IMultiRequest(
                                                    VolumesSchemaRequests.create(materializer).volume(leaf).delete())),
                                    Functions.constant(Boolean.FALSE), 
                                    SameThreadExecutor.getInstance());
                        }
                    } finally {
                        materializer.cache().lock().readLock().unlock();
                    }
                }
                return Futures.immediateFuture(Boolean.valueOf(Boolean.FALSE));
            }
        }
    }

    public static final class CachedXomega implements Callable<Optional<UnsignedLong>> {

        public static CachedXomega create(
                VersionedId volume,
                LockableZNodeCache<ControlZNode<?>,?,?> cache) {
            return new CachedXomega(volume, cache);
        }
        
        private final VersionedId volume;
        private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
        
        protected CachedXomega(
                VersionedId volume,
                LockableZNodeCache<ControlZNode<?>,?,?> cache) {
            this.volume = volume;
            this.cache = cache;
        }
        
        public AbsoluteZNodePath path() {
            return ControlSchema.Safari.Volumes.Volume.Log.Version.Xomega.pathOf(volume.getValue(), volume.getVersion());
        }

        @Override
        public Optional<UnsignedLong> call() {
            cache.lock().readLock().lock();
            try {
                ControlZNode<?> node = cache.cache().get(path());
                if ((node != null) && (node.data().stamp() > 0L)) {
                    return Optional.of((UnsignedLong) node.data().get());
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            return Optional.absent();
        }
    }
    
    public static final class GetXomega extends SimpleToStringListenableFuture<Optional<UnsignedLong>> {
        
        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<UnsignedLong>> create(
                final VersionedId volume,
                final Materializer<ControlZNode<?>,O> materializer) {
            return new GetXomega(Call.create(volume, materializer));
        }
        
        protected GetXomega(
                ListenableFuture<Optional<UnsignedLong>> future) { 
            super(future);
        }
        
        protected static class Call<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> implements Callable<Optional<Optional<UnsignedLong>>> {

            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<UnsignedLong>> create(
                    final VersionedId volume,
                    final Materializer<ControlZNode<?>,O> materializer) {
                final CachedXomega cached = CachedXomega.create(volume, materializer.cache());
                final ZNodePath path = cached.path();
                @SuppressWarnings("unchecked")
                final SubmittedRequests<? extends Records.Request, O> query = SubmittedRequests.submit(
                        materializer, 
                        PathToRequests.forRequests(
                                Operations.Requests.sync(),
                                Operations.Requests.getData()).apply(path));
                CallablePromiseTask<Call<O>,Optional<UnsignedLong>> task = CallablePromiseTask.create(
                        new Call<O>(cached, query), 
                        SettableFuturePromise.<Optional<UnsignedLong>>create());
                task.task().addListener(task, SameThreadExecutor.getInstance());
                return task;
            }
            
            private final CachedXomega cached;
            
            protected Call(
                    CachedXomega cached,
                    ListenableFuture<List<O>> future) {
                super(future);
                this.cached = cached;
            }
            
            @Override
            public Optional<Optional<UnsignedLong>> call() throws Exception {
                if (isDone()) {
                    List<O> responses = get();
                    int index;
                    Optional<Operation.Error> error = null;
                    for (index=0; index<responses.size(); ++index) {
                        error = Operations.maybeError(responses.get(index).record(), KeeperException.Code.NONODE);
                    }
                    Optional<UnsignedLong> xomega;
                    if (error.isPresent()) {
                        xomega = Optional.<UnsignedLong>absent();
                    } else {
                        xomega = cached.call();
                    }
                    return Optional.of(xomega);
                }
                return Optional.absent();
            }
        }
    }

    public static final class CreateXomega extends SimpleToStringListenableFuture<UnsignedLong> {

        public static Create creator(
                final VersionedId volume,
                final Materializer<ControlZNode<?>,?> materializer) {
            return Create.create(volume, materializer);
        }
        
        protected CreateXomega(ListenableFuture<UnsignedLong> future) {
            super(future);
        }
        
        protected static class Create implements AsyncFunction<UnsignedLong,UnsignedLong> {

            public static Create create(
                    final VersionedId volume,
                    final Materializer<ControlZNode<?>,?> materializer) {
                final CachedXomega cached = CachedXomega.create(volume, materializer.cache());
                return new Create(cached, materializer);
            }
            
            private final CachedXomega cached;
            private final Materializer<ControlZNode<?>,?> materializer;

            protected Create(
                    CachedXomega cached,
                    Materializer<ControlZNode<?>,?> materializer) {
                this.cached = cached;
                this.materializer = materializer;
            }
            
            @Override
            public CreateXomega apply(final UnsignedLong input) throws Exception {
                Optional<UnsignedLong> xomega = cached.call();
                ListenableFuture<UnsignedLong> future;
                if (xomega.isPresent()) {
                    checkArgument(input.equals(xomega.get()));
                    future = Futures.immediateFuture(input);
                } else {
                    future = Futures.transform(
                            materializer.create(cached.path(), input).call(),
                            new AsyncFunction<Operation.ProtocolResponse<?>, UnsignedLong>() {
                                @Override
                                public ListenableFuture<UnsignedLong> apply(Operation.ProtocolResponse<?> response) throws Exception {
                                    Operations.unlessError(response.record());
                                    return Futures.immediateFuture(input);
                                }
                            }, SameThreadExecutor.getInstance());
                }
                return new CreateXomega(future);
            }   
        }
    }
    
    public static final class CommitSnapshot extends SimpleToStringListenableFuture<Boolean> {
        
        public static ListenableFuture<Boolean> create(
                final Identifier volume,
                final ClientExecutor<? super Records.Request,?,?> client,
                final ByteCodec<Object> codec) throws IOException {
            final ZNodePath path = StorageSchema.Safari.Volumes.Volume.Snapshot.Commit.pathOf(volume);
            return new CommitSnapshot(
                    Futures.transform(
                    SubmittedRequest.submit(client, Operations.Requests.create().setPath(path).setData(codec.toBytes(Boolean.TRUE)).build()),
                    CALLBACK,
                    SameThreadExecutor.getInstance()));
        }
        
        protected CommitSnapshot(ListenableFuture<Boolean> future) {
            super(future);
        }

        private static AsyncFunction<Operation.ProtocolResponse<?>, Boolean> CALLBACK = new AsyncFunction<Operation.ProtocolResponse<?>, Boolean>() {
            @Override
            public ListenableFuture<Boolean> apply(
                    Operation.ProtocolResponse<?> response) throws Exception {
                Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS, KeeperException.Code.NONODE);
                return Futures.immediateFuture(Boolean.TRUE);
            }
        };
    }
    
    protected static abstract class ExecuteSnapshot implements Function<List<ListenableFuture<?>>, Optional<? extends ListenableFuture<?>>> {

        public static <O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> ListenableFuture<?> execute(
                final EnsembleView<ServerInetAddressView> fromEnsemble,
                final Materializer<StorageZNode<?>,?> fromMaterializer,
                final Pair<ServerInetAddressView, ? extends T> fromClient,
                final Pair<ServerInetAddressView, ? extends T> toClient,
                final List<ZNodePath> paths,
                final PromiseTask<VolumeOperationDirective, Boolean> request,
                final Materializer<ControlZNode<?>,?> control,
                final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
            return execute(request.task().isCommit(), fromEnsemble, fromMaterializer, fromClient, toClient, paths, request, control, anonymous);
        }
        
        public static <O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> ListenableFuture<?> execute(
                final boolean isCommit,
                final EnsembleView<ServerInetAddressView> fromEnsemble,
                final Materializer<StorageZNode<?>,?> fromMaterializer,
                final Pair<ServerInetAddressView, ? extends T> fromClient,
                final Pair<ServerInetAddressView, ? extends T> toClient,
                final List<ZNodePath> paths,
                final PromiseTask<VolumeOperationDirective, Boolean> request,
                final Materializer<ControlZNode<?>,?> control,
                final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
            final VersionedId volume = request.task().getOperation().getVolume();
            final Identifier fromVolume = request.task().getOperation().getVolume().getValue();
            final Identifier toVolume;
            final ZNodeName fromBranch;
            final ZNodeName toBranch;
            switch (request.task().getOperation().getOperator().getOperator()) {
            case MERGE:
            {
                MergeParameters parameters = (MergeParameters) request.task().getOperation().getOperator().getParameters();
                toVolume = parameters.getParent().getValue();
                toBranch = paths.get(0).suffix(paths.get(1));
                fromBranch = ZNodeLabel.empty();
                break;
            }
            case SPLIT:
            {
                SplitParameters parameters = (SplitParameters) request.task().getOperation().getOperator().getParameters();
                toVolume = parameters.getLeaf();
                toBranch = ZNodeLabel.empty();
                fromBranch = parameters.getBranch();
                break;
            }
            case TRANSFER:
            {
                toVolume = fromVolume;
                toBranch = ZNodeLabel.empty();
                fromBranch = ZNodeLabel.empty();
                break;
            }
            default:
                throw new AssertionError();
            }
            final Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot = new Callable<Snapshot.DeleteExistingSnapshot>() {
                @Override
                public Snapshot.DeleteExistingSnapshot call() throws Exception {
                    return Snapshot.undo(
                            toClient.second(), 
                            toVolume, 
                            toBranch);
                }
            };
            if (isCommit) {
                final Callable<ListenableFuture<Optional<UnsignedLong>>> getXomega = new Callable<ListenableFuture<Optional<UnsignedLong>>>() {
                    @Override
                    public ListenableFuture<Optional<UnsignedLong>> call()
                            throws Exception {
                        return GetXomega.create(volume, control);
                    }
                };
                final AsyncFunction<UnsignedLong, UnsignedLong> createXomega = CreateXomega.creator(volume, control);
                final Callable<ListenableFuture<UnsignedLong>> doSnapshot = new Callable<ListenableFuture<UnsignedLong>>() {
                    @Override
                    public ListenableFuture<UnsignedLong> call() throws Exception {
                        return Snapshot.recover(
                                fromClient.second(), 
                                fromVolume, 
                                fromBranch, 
                                toClient.second(), 
                                toVolume, 
                                toBranch, 
                                fromMaterializer, 
                                fromClient.first(), 
                                fromEnsemble, 
                                anonymous);
                    }
                };
                final Callable<ListenableFuture<Boolean>> commitSnapshot = new Callable<ListenableFuture<Boolean>>() {
                    @Override
                    public ListenableFuture<Boolean> call() throws Exception {
                        return CommitSnapshot.create(toVolume, toClient.second(), fromMaterializer.codec());
                    }
                };
                final Callable<ListenableFuture<?>> deleteVolume = new Callable<ListenableFuture<?>>() {
                    @Override
                    public ListenableFuture<?> call() throws Exception {
                        AbsoluteZNodePath path = StorageSchema.Safari.Volumes.Volume.pathOf(request.task().getOperation().getVolume().getValue());
                        if (fromBranch instanceof EmptyZNodeLabel) {
                            return DeleteSubtree.deleteAll(path, fromClient.second());
                        } else {
                            return DeleteSubtree.deleteChildren(path.join(StorageSchema.Safari.Volumes.Volume.Root.LABEL).join(fromBranch), fromClient.second());
                        }
                    }
                };
                return doSnapshot(
                        getXomega, 
                        createXomega, 
                        doSnapshot, 
                        commitSnapshot,
                        deleteVolume, 
                        request, 
                        undoSnapshot, 
                        paths, 
                        control);
            } else {
                return undoSnapshot(request, undoSnapshot, paths, control);
            }
        }
        
        public static ListenableFuture<Boolean> doSnapshot(
                Callable<ListenableFuture<Optional<UnsignedLong>>> getXomega,
                AsyncFunction<UnsignedLong, UnsignedLong> createXomega,
                Callable<ListenableFuture<UnsignedLong>> doSnapshot,
                Callable<ListenableFuture<Boolean>> commitSnapshot,
                Callable<ListenableFuture<?>> deleteVolume,
                PromiseTask<VolumeOperationDirective, Boolean> request,
                Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot,
                List<ZNodePath> paths,
                Materializer<ControlZNode<?>,?> control) {
            return DoSnapshot.create(
                    getXomega, 
                    createXomega, 
                    doSnapshot, 
                    commitSnapshot,
                    deleteVolume, 
                    request, 
                    undoSnapshot, 
                    paths, 
                    control);
        }
        
        public static ListenableFuture<Boolean> undoSnapshot(
                PromiseTask<VolumeOperationDirective, Boolean> request,
                Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot,
                List<ZNodePath> paths,
                Materializer<ControlZNode<?>,?> control) {
            return UndoSnapshot.create(request, undoSnapshot, paths, control);
        }

        protected final Materializer<ControlZNode<?>,?> control;
        protected final PromiseTask<VolumeOperationDirective, Boolean> request;
        protected final Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot;
        protected final List<ZNodePath> paths;
        
        protected ExecuteSnapshot(
                PromiseTask<VolumeOperationDirective, Boolean> request,
                Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot,
                List<ZNodePath> paths,
                Materializer<ControlZNode<?>,?> control) {
            this.request = request;
            this.undoSnapshot = undoSnapshot;
            this.paths = paths;
            this.control = control;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(request).toString();
        }
    }

    protected static final class DoSnapshot extends ExecuteSnapshot {

        protected static ListenableFuture<Boolean> create(
                Callable<ListenableFuture<Optional<UnsignedLong>>> getXomega,
                AsyncFunction<UnsignedLong, UnsignedLong> createXomega,
                Callable<ListenableFuture<UnsignedLong>> doSnapshot,
                Callable<ListenableFuture<Boolean>> commitSnapshot,
                Callable<ListenableFuture<?>> deleteVolume,
                PromiseTask<VolumeOperationDirective, Boolean> request,
                Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot,
                List<ZNodePath> paths,
                Materializer<ControlZNode<?>,?> control) {
            return ChainedFutures.run(
                    ChainedFutures.process(
                            ChainedFutures.chain(
                                    new DoSnapshot(
                                            getXomega,
                                            createXomega,
                                            doSnapshot,
                                            deleteVolume,
                                            commitSnapshot,
                                            request,
                                            undoSnapshot,
                                            paths, 
                                            control), 
                                    Lists.<ListenableFuture<?>>newArrayListWithCapacity(6)), 
                            ChainedFutures.<Boolean>castLast()), 
                    request);
            
        }
        
        private final Callable<ListenableFuture<Optional<UnsignedLong>>> getXomega;
        private final AsyncFunction<UnsignedLong, UnsignedLong> createXomega;
        private final Callable<ListenableFuture<UnsignedLong>> doSnapshot;
        private final Callable<ListenableFuture<Boolean>> commitSnapshot;
        private final Callable<ListenableFuture<?>> deleteVolume;
        
        protected DoSnapshot(
                Callable<ListenableFuture<Optional<UnsignedLong>>> getXomega,
                AsyncFunction<UnsignedLong, UnsignedLong> createXomega,
                Callable<ListenableFuture<UnsignedLong>> doSnapshot,
                Callable<ListenableFuture<?>> deleteVolume,
                Callable<ListenableFuture<Boolean>> commitSnapshot,
                PromiseTask<VolumeOperationDirective, Boolean> request,
                Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot,
                List<ZNodePath> paths,
                Materializer<ControlZNode<?>,?> control) {
            super(request, undoSnapshot, paths, control);
            this.getXomega = getXomega;
            this.createXomega = createXomega;
            this.doSnapshot = doSnapshot;
            this.commitSnapshot = commitSnapshot;
            this.deleteVolume = deleteVolume;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                List<ListenableFuture<?>> input) {
            switch (input.size()) {
            case 0:
            {
                ListenableFuture<?> future;
                try {
                    future = getXomega.call();
                } catch (Exception e) {
                    future = Futures.immediateFuture(e);
                }
                return Optional.of(future);
            }
            case 1:
            {
                Optional<UnsignedLong> xomega;
                try {
                    xomega = (Optional<UnsignedLong>) input.get(input.size()-1).get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
                ListenableFuture<UnsignedLong> future;
                if (xomega.isPresent()) {
                    future = Futures.immediateFuture(xomega.get());
                } else {
                    try {
                        future = doSnapshot.call();
                    } catch (Exception e) {
                        future = Futures.immediateFailedFuture(e);
                    }
                }
                return Optional.of(future);
            }
            case 2:
            {
                UnsignedLong xomega;
                try {
                    xomega = (UnsignedLong) input.get(input.size()-1).get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.of(undoSnapshot(request, undoSnapshot, paths, control));
                }
                ListenableFuture<UnsignedLong> future;
                try {
                    future = createXomega.apply(xomega);
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            case 3:
            {
                Object last;
                try {
                    last = input.get(input.size()-1).get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
                if (last instanceof Boolean) {
                    return Optional.absent();
                } else {
                    assert (last instanceof UnsignedLong);
                    ListenableFuture<Boolean> future;
                    try {
                        future = commitSnapshot.call();
                    } catch (Exception e) {
                        future = Futures.immediateFailedFuture(e);
                    }
                    return Optional.of(future);
                }
            }
            case 4:
            {
                ListenableFuture<?> future;
                try {
                    future = deleteVolume.call();
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            case 5:
            {
                try {
                    input.get(input.size()-1).get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
                return Optional.of(Futures.immediateFuture(Boolean.TRUE));
            }
            case 6:
                return Optional.absent();
            default:
                throw new AssertionError();
            }
        }
    }
    
    protected static final class UndoSnapshot extends ExecuteSnapshot {

        protected static ListenableFuture<Boolean> create(
                PromiseTask<VolumeOperationDirective, Boolean> request,
                Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot,
                List<ZNodePath> paths,
                Materializer<ControlZNode<?>,?> control) {
            return ChainedFutures.run(
                    ChainedFutures.process(
                            ChainedFutures.chain(
                                    new UndoSnapshot(request, undoSnapshot, paths, control), 
                                    Lists.<ListenableFuture<?>>newArrayListWithCapacity(3)), 
                            ChainedFutures.<Boolean>castLast()), 
                    request);
        }
        
        protected UndoSnapshot(
                PromiseTask<VolumeOperationDirective, Boolean> request,
                Callable<Snapshot.DeleteExistingSnapshot> undoSnapshot,
                List<ZNodePath> paths,
                Materializer<ControlZNode<?>,?> control) {
            super(request, undoSnapshot, paths, control);
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                List<ListenableFuture<?>> input) {
            Optional<? extends ListenableFuture<?>> last = input.isEmpty() ? Optional.<ListenableFuture<?>>absent() : Optional.of(input.get(input.size()-1));
            if (!last.isPresent()) {
                ListenableFuture<?> future;
                try {
                    future = undoSnapshot.call();
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            } else {
                try {
                    last.get().get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
                if (input.size() == 1) {
                    return Optional.of(UndoOperation.create(control, paths, request));
                } else {
                    return Optional.absent();
                }
            }
        }
    }
}
