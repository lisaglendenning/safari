package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;
import edu.uw.zookeeper.safari.storage.Storage;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class CoordinateSnapshot implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain<ListenableFuture<?>>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton @Snapshot
        public AsyncFunction<Boolean, Boolean> getCoordinateSnapshot(
                final VolumeOperation<?> operation,
                final @Snapshot AsyncFunction<Boolean,Long> snapshot,
                final @From SnapshotVolumeParameters fromVolume,
                final @To SnapshotVolumeParameters toVolume,
                final @From EnsembleView<ServerInetAddressView> fromEnsemble,
                final @Storage Materializer<StorageZNode<?>,?> fromMaterializer,
                final @From ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> fromClient,
                final @To ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> toClient) {
            return new AsyncFunction<Boolean, Boolean>() {
                @Override
                public ListenableFuture<Boolean> apply(Boolean input) throws Exception {
                    if (input.booleanValue()) {
                    return CoordinateSnapshot.commit(
                            operation,
                            fromVolume,
                            toVolume,
                            snapshot,
                            fromMaterializer,
                            fromClient,
                            toClient);
                    } else {
                        return CoordinateSnapshot.abort(snapshot);
                    }
                }
            };
        }

        @Override
        public Key<?> getKey() {
            return Key.get(new TypeLiteral<Callable<ListenableFuture<Boolean>>>(){});
        }

        @Override
        protected void configure() {
        }
    }
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Boolean> commit(
            final VolumeOperation<?> operation,
            final SnapshotVolumeParameters fromVolume,
            final SnapshotVolumeParameters toVolume,
            final AsyncFunction<Boolean,Long> snapshot,
            final Materializer<StorageZNode<?>,?> fromMaterializer,
            final ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> fromClient,
            final ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?> toClient) {
        final Callable<ListenableFuture<Optional<Long>>> getXomega = new Callable<ListenableFuture<Optional<Long>>>() {
            @Override
            public ListenableFuture<Optional<Long>> call()
                    throws Exception {
                return GetXomega.create(operation.getVolume(), fromMaterializer);
            }
        };
        final AsyncFunction<Long, KeeperException.Code> createXomega = CreateXomega.creator(operation.getVolume(), fromMaterializer.codec(), fromMaterializer);
        final Callable<ListenableFuture<Long>> commitSnapshot = new Callable<ListenableFuture<Long>>() {
            @Override
            public ListenableFuture<Long> call() throws Exception {
                return CommitSnapshot.create(toVolume.getVolume(), toVolume.getVersion(), toClient, fromMaterializer.codec());
            }
        };
        final Callable<ListenableFuture<List<AbsoluteZNodePath>>> deleteVolume = new Callable<ListenableFuture<List<AbsoluteZNodePath>>>() {
            @Override
            public ListenableFuture<List<AbsoluteZNodePath>> call() throws Exception {
                return DeleteSubtree.deleteChildren(
                        StorageSchema.Safari.Volumes.Volume.Root.pathOf(fromVolume.getVolume()).join(fromVolume.getBranch()), 
                        fromClient);
            }
        };
        final AsyncFunction<? super Long, List<AbsoluteZNodePath>> callDelete = new AsyncFunction<Object, List<AbsoluteZNodePath>>() {
            @Override
            public ListenableFuture<List<AbsoluteZNodePath>> apply(Object input) throws Exception {
                return deleteVolume.call();
            }
        };
        final AsyncFunction<? super Long, List<AbsoluteZNodePath>> complete;
        if (operation.getOperator().getOperator() == VolumeOperator.MERGE) {
            final CreateXomega.Create creator = CreateXomega.creator(((MergeParameters) operation.getOperator().getParameters()).getParent(), fromMaterializer.codec(), toClient);
            complete = new AsyncFunction<Long, List<AbsoluteZNodePath>>() {
                @Override
                public ListenableFuture<List<AbsoluteZNodePath>> apply(Long input) throws Exception {
                    return Futures.transform(
                            creator.apply(input),
                            new AsyncFunction<KeeperException.Code, List<AbsoluteZNodePath>>() {
                                @Override
                                public ListenableFuture<List<AbsoluteZNodePath>> apply(
                                        KeeperException.Code input)
                                        throws Exception {
                                    switch (input) {
                                    case OK:
                                    case NODEEXISTS:
                                        break;
                                    default:
                                        throw KeeperException.create(input);
                                    }
                                    return deleteVolume.call();
                                }
                            });
                }
            };
        } else {
            complete = callDelete;
        }
        return commit(
                snapshot,
                getXomega, 
                createXomega,
                commitSnapshot,
                complete);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Boolean> abort(
            final AsyncFunction<Boolean,Long> snapshot) {
        try {
            return abort(snapshot.apply(Boolean.FALSE));
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }
    
    protected static ListenableFuture<Boolean> commit(
            AsyncFunction<Boolean,Long> snapshot,
            Callable<ListenableFuture<Optional<Long>>> getXomega,
            AsyncFunction<Long, KeeperException.Code> createXomega,
            Callable<ListenableFuture<Long>> commitSnapshot,
            AsyncFunction<? super Long, ?> complete) {
        return ChainedFutures.run(
                ChainedFutures.<Boolean>castLast(
                        ChainedFutures.arrayList(
                                new CoordinateSnapshot(
                                        snapshot,
                                        getXomega,
                                        createXomega,
                                        commitSnapshot,
                                        complete), 
                                5)));
    }

    protected static ListenableFuture<Boolean> abort(
            ListenableFuture<Long> delete) {
        return Futures.transform(
                delete, 
                Functions.constant(Boolean.FALSE));
    }

    private final AsyncFunction<Boolean,Long> snapshot;
    private final Callable<ListenableFuture<Optional<Long>>> getXomega;
    private final AsyncFunction<Long, KeeperException.Code> createXomega;
    private final Callable<ListenableFuture<Long>> commitSnapshot;
    private final AsyncFunction<? super Long, ?> complete;
    
    protected CoordinateSnapshot(
            AsyncFunction<Boolean,Long> snapshot,
            Callable<ListenableFuture<Optional<Long>>> getXomega,
            AsyncFunction<Long, KeeperException.Code> createXomega,
            Callable<ListenableFuture<Long>> commitSnapshot,
            AsyncFunction<? super Long, ?> complete) {
        this.snapshot = snapshot;
        this.getXomega = getXomega;
        this.createXomega = createXomega;
        this.commitSnapshot = commitSnapshot;
        this.complete = complete;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<? extends ListenableFuture<?>> apply(
            FutureChain<ListenableFuture<?>> input) {
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
            Optional<Long> xomega;
            try {
                xomega = (Optional<Long>) input.getLast().get();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
            ListenableFuture<?> future;
            if (xomega.isPresent()) {
                future = CallSnapshot.create(Futures.immediateFuture(xomega.get()));
            } else {
                future = CallSnapshot.create(new CallSnapshotChain());
            }
            return Optional.of(future);
        }
        case 2:
        {
            try {
                input.getLast().get();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
            ListenableFuture<?> future;
            try {
                future = commitSnapshot.call();
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return Optional.of(future);
        }
        case 3:
        {
            Long toXomega;
            try {
                toXomega = (Long) input.getLast().get();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
            ListenableFuture<?> future;
            try {
                future = complete.apply(toXomega);
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return Optional.of(future);
        }
        case 4:
        {
            try {
                input.getLast().get();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
            return Optional.of(Futures.immediateFuture(Boolean.TRUE));
        }
        case 5:
            return Optional.absent();
        default:
            throw new AssertionError();
        }
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }

    protected static final class CallSnapshot extends SimpleToStringListenableFuture<Long> {
        public static CallSnapshot create(CallSnapshotChain chain) {
            return create(
                    ChainedFutures.run(
                            ChainedFutures.<Long>castLast(
                                    ChainedFutures.arrayList(
                                            chain, 3))));
        }

        public static CallSnapshot create(ListenableFuture<Long> future) {
            return new CallSnapshot(future);
        }
        
        protected CallSnapshot(ListenableFuture<Long> delegate) {
            super(delegate);
        }
    }

    protected final class CallSnapshotChain implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, ChainedFutures.ListChain<ListenableFuture<?>,?>> {
        
        public CallSnapshotChain() {}
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                ChainedFutures.ListChain<ListenableFuture<?>,?> input) {
            switch (input.size()) {
            case 0:
            {
                ListenableFuture<?> future;
                try {
                    future = snapshot.apply(Boolean.TRUE);
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            case 1:
            {
                Long xomega;
                try {
                    xomega = (Long) input.getLast().get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.of(abort(snapshot));
                }
                ListenableFuture<KeeperException.Code> future;
                try {
                    future = createXomega.apply(xomega);
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            case 2:
            {
                Object last;
                try {
                    last = input.getLast().get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
                ListenableFuture<?> future;
                if (last instanceof Boolean) {
                    assert (Boolean.FALSE.equals(last));
                    future = input.get(0);
                } else {
                    KeeperException.Code code = (KeeperException.Code) last;
                    if (code != KeeperException.Code.OK) {
                        future = Futures.immediateFailedFuture(KeeperException.create(code));
                    } else {
                        future = input.get(0);
                    }
                }
                return Optional.of(future);
            }
            case 3:
                return Optional.absent();
            default:
                throw new AssertionError();
            }
        }
    }
    
    public static final class CachedXomega implements Callable<Optional<Long>> {

        public static CachedXomega create(
                VersionedId volume,
                LockableZNodeCache<StorageZNode<?>,?,?> cache) {
            return new CachedXomega(volume, cache);
        }
        
        private final VersionedId volume;
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        
        protected CachedXomega(
                VersionedId volume,
                LockableZNodeCache<StorageZNode<?>,?,?> cache) {
            this.volume = volume;
            this.cache = cache;
        }
        
        public AbsoluteZNodePath path() {
            return StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.pathOf(volume.getValue(), volume.getVersion());
        }

        @Override
        public Optional<Long> call() {
            cache.lock().readLock().lock();
            try {
                StorageZNode<?> node = cache.cache().get(path());
                if ((node != null) && (node.data().stamp() > 0L)) {
                    return Optional.of((Long) node.data().get());
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            return Optional.absent();
        }
    }

    public static final class GetXomega extends SimpleToStringListenableFuture<Optional<Long>> {
        
        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<Long>> create(
                final VersionedId volume,
                final Materializer<StorageZNode<?>,O> materializer) {
            return new GetXomega(Call.create(volume, materializer));
        }
        
        protected GetXomega(
                ListenableFuture<Optional<Long>> future) { 
            super(future);
        }
        
        protected static class Call<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> implements Callable<Optional<Optional<Long>>> {

            public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Optional<Long>> create(
                    final VersionedId volume,
                    final Materializer<StorageZNode<?>,O> materializer) {
                final CachedXomega cached = CachedXomega.create(volume, materializer.cache());
                Optional<Long> value = cached.call();
                if (value.isPresent()) {
                    return Futures.immediateFuture(value);
                }
                final ZNodePath path = cached.path();
                @SuppressWarnings("unchecked")
                final SubmittedRequests<? extends Records.Request, O> query = SubmittedRequests.submit(
                        materializer, 
                        PathToRequests.forRequests(
                                Operations.Requests.sync(),
                                Operations.Requests.getData()).apply(path));
                CallablePromiseTask<Call<O>,Optional<Long>> task = CallablePromiseTask.listen(
                        new Call<O>(cached, query), 
                        SettableFuturePromise.<Optional<Long>>create());
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
            public Optional<Optional<Long>> call() throws Exception {
                if (isDone()) {
                    List<O> responses = get();
                    int index;
                    Optional<Operation.Error> error = null;
                    for (index=0; index<responses.size(); ++index) {
                        error = Operations.maybeError(responses.get(index).record(), KeeperException.Code.NONODE);
                    }
                    Optional<Long> xomega;
                    if (error.isPresent()) {
                        xomega = Optional.<Long>absent();
                    } else {
                        xomega = cached.call();
                    }
                    return Optional.of(xomega);
                }
                return Optional.absent();
            }
        }
    }

    public static final class CreateXomega extends SimpleToStringListenableFuture<KeeperException.Code> {

        public static Create creator(
                final VersionedId volume,
                final Serializers.ByteSerializer<Object> serializer,
                final ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> client) {
            return Create.create(volume, serializer, client);
        }
        
        protected CreateXomega(ListenableFuture<KeeperException.Code> future) {
            super(future);
        }
        
        protected static class Create implements AsyncFunction<Long,KeeperException.Code> {

            public static Create create(
                    final VersionedId volume,
                    final Serializers.ByteSerializer<Object> serializer,
                    final ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> client) {
                return new Create(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.pathOf(volume.getValue(), volume.getVersion()), 
                        serializer, 
                        client);
            }
            
            private final ZNodePath path;
            private final Serializers.ByteSerializer<Object> serializer;
            private final ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> client;

            protected Create(
                    ZNodePath path,
                    Serializers.ByteSerializer<Object> serializer,
                    ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> client) {
                this.path = path;
                this.serializer = serializer;
                this.client = client;
            }
            
            @Override
            public ListenableFuture<KeeperException.Code> apply(final Long input) throws Exception {
                return Futures.transform(
                        client.submit(Operations.Requests.serialized(Operations.Requests.create().setPath(path), serializer, input).build()),
                        new Function<Operation.ProtocolResponse<?>, KeeperException.Code>() {
                            @Override
                            public KeeperException.Code apply(Operation.ProtocolResponse<?> response) {
                                if (response.record() instanceof Operation.Error) {
                                    return ((Operation.Error) response.record()).error();
                                } else {
                                    return KeeperException.Code.OK;
                                }
                            }
                        });
            }   
        }
    }

    public static final class CommitSnapshot extends SimpleToStringListenableFuture<Long> {
        
        public static ListenableFuture<Long> create(
                final Identifier volume,
                final UnsignedLong version,
                final ClientExecutor<? super Records.Request,?,?> client,
                final ByteCodec<Object> codec) throws IOException {
            final ZNodePath path = StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.pathOf(volume, version);
            return new CommitSnapshot(
                    Futures.transform(
                            SubmittedRequest.submit(
                                    client, 
                                    Operations.Requests.create().setPath(path).setData(codec.toBytes(Boolean.TRUE)).build()),
                            CALLBACK));
        }
        
        protected CommitSnapshot(ListenableFuture<Long> future) {
            super(future);
        }

        private static AsyncFunction<Operation.ProtocolResponse<?>, Long> CALLBACK = new AsyncFunction<Operation.ProtocolResponse<?>, Long>() {
            @Override
            public ListenableFuture<Long> apply(
                    Operation.ProtocolResponse<?> response) throws Exception {
                Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS, KeeperException.Code.NONODE);
                return Futures.immediateFuture(Long.valueOf(response.zxid()));
            }
        };
    }
}
