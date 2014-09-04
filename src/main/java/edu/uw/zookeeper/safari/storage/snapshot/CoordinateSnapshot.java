package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ChainedFutures.ChainedProcessor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class CoordinateSnapshot implements ChainedProcessor<ListenableFuture<?>> {

    public static <O extends Operation.ProtocolResponse<?>, T extends ConnectionClientExecutor<? super Records.Request, O, SessionListener, ?>> ListenableFuture<Boolean> call(
            final boolean isCommit,
            final VolumeOperation<?> operation,
            final EnsembleView<ServerInetAddressView> fromEnsemble,
            final Materializer<StorageZNode<?>,?> fromMaterializer,
            final Pair<ServerInetAddressView, ? extends T> fromClient,
            final Pair<ServerInetAddressView, ? extends T> toClient,
            final List<ZNodePath> paths,
            final ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> anonymous) {
        final Identifier fromVolume = operation.getVolume().getValue();
        final Identifier toVolume;
        final ZNodeName fromBranch;
        final ZNodeName toBranch;
        switch (operation.getOperator().getOperator()) {
        case MERGE:
        {
            MergeParameters parameters = (MergeParameters) operation.getOperator().getParameters();
            toVolume = parameters.getParent().getValue();
            toBranch = paths.get(0).suffix(paths.get(1));
            fromBranch = ZNodeLabel.empty();
            break;
        }
        case SPLIT:
        {
            SplitParameters parameters = (SplitParameters) operation.getOperator().getParameters();
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
        final Callable<ExecuteSnapshot.DeleteExistingSnapshot> undoSnapshot = new Callable<ExecuteSnapshot.DeleteExistingSnapshot>() {
            @Override
            public ExecuteSnapshot.DeleteExistingSnapshot call() throws Exception {
                return ExecuteSnapshot.undo(
                        toClient.second(), 
                        toVolume, 
                        operation.getOperator().getParameters().getVersion(),
                        toBranch);
            }
        };
        if (isCommit) {
            final Callable<ListenableFuture<Optional<Long>>> getXomega = new Callable<ListenableFuture<Optional<Long>>>() {
                @Override
                public ListenableFuture<Optional<Long>> call()
                        throws Exception {
                    return GetXomega.create(operation.getVolume(), fromMaterializer);
                }
            };
            final AsyncFunction<Long, KeeperException.Code> createXomega = CreateXomega.creator(operation.getVolume(), fromMaterializer);
            final Callable<ListenableFuture<Long>> doSnapshot = new Callable<ListenableFuture<Long>>() {
                @Override
                public ListenableFuture<Long> call() throws Exception {
                    return ExecuteSnapshot.recover(
                            fromClient.second(), 
                            fromVolume, 
                            fromBranch, 
                            toClient.second(), 
                            toVolume, 
                            operation.getOperator().getParameters().getVersion(),
                            toBranch, 
                            fromMaterializer, 
                            fromClient.first(), 
                            fromEnsemble, 
                            anonymous);
                }
            };
            final Callable<ListenableFuture<Long>> commitSnapshot = new Callable<ListenableFuture<Long>>() {
                @Override
                public ListenableFuture<Long> call() throws Exception {
                    return CommitSnapshot.create(toVolume, operation.getOperator().getParameters().getVersion(), toClient.second(), fromMaterializer.codec());
                }
            };
            final Callable<ListenableFuture<List<AbsoluteZNodePath>>> deleteVolume = new Callable<ListenableFuture<List<AbsoluteZNodePath>>>() {
                @Override
                public ListenableFuture<List<AbsoluteZNodePath>> call() throws Exception {
                    return DeleteSubtree.deleteChildren(
                            StorageSchema.Safari.Volumes.Volume.Root.pathOf(toVolume).join(fromBranch), 
                            fromClient.second());
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
                final CreateXomega.Create creator = CreateXomega.creator(((MergeParameters) operation.getOperator().getParameters()).getParent(), fromMaterializer);
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
                                }, SameThreadExecutor.getInstance());
                    }
                };
            } else {
                complete = callDelete;
            }
            return doSnapshot(
                    getXomega, 
                    createXomega, 
                    doSnapshot, 
                    commitSnapshot,
                    complete,
                    undoSnapshot);
        } else {
            return undoSnapshot(undoSnapshot);
        }
    }
    
    protected static ListenableFuture<Boolean> doSnapshot(
            Callable<ListenableFuture<Optional<Long>>> getXomega,
            AsyncFunction<Long, KeeperException.Code> createXomega,
            Callable<ListenableFuture<Long>> doSnapshot,
            Callable<ListenableFuture<Long>> commitSnapshot,
            AsyncFunction<? super Long, ?> complete,
            Callable<ExecuteSnapshot.DeleteExistingSnapshot> undoSnapshot) {
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(
                                new CoordinateSnapshot(
                                        getXomega,
                                        createXomega,
                                        doSnapshot,
                                        commitSnapshot,
                                        complete,
                                        undoSnapshot), 
                                Lists.<ListenableFuture<?>>newArrayListWithCapacity(6)), 
                        ChainedFutures.<Boolean>castLast()), 
                        SettableFuturePromise.<Boolean>create());
    }
    
    protected static ListenableFuture<Boolean> undoSnapshot(
            Callable<ExecuteSnapshot.DeleteExistingSnapshot> delete) {
        try {
            return undoSnapshot(delete.call());
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    protected static ListenableFuture<Boolean> undoSnapshot(
            ExecuteSnapshot.DeleteExistingSnapshot delete) {
        return Futures.transform(
                delete, 
                Functions.constant(Boolean.FALSE),
                SameThreadExecutor.getInstance());
    }

    private final Callable<ExecuteSnapshot.DeleteExistingSnapshot> undoSnapshot;
    private final Callable<ListenableFuture<Optional<Long>>> getXomega;
    private final AsyncFunction<Long, KeeperException.Code> createXomega;
    private final Callable<ListenableFuture<Long>> doSnapshot;
    private final Callable<ListenableFuture<Long>> commitSnapshot;
    private final AsyncFunction<? super Long, ?> complete;
    
    protected CoordinateSnapshot(
            Callable<ListenableFuture<Optional<Long>>> getXomega,
            AsyncFunction<Long, KeeperException.Code> createXomega,
            Callable<ListenableFuture<Long>> doSnapshot,
            Callable<ListenableFuture<Long>> commitSnapshot,
            AsyncFunction<? super Long, ?> complete,
            Callable<ExecuteSnapshot.DeleteExistingSnapshot> undoSnapshot) {
        this.undoSnapshot = undoSnapshot;
        this.getXomega = getXomega;
        this.createXomega = createXomega;
        this.doSnapshot = doSnapshot;
        this.commitSnapshot = commitSnapshot;
        this.complete = complete;
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
            Optional<Long> xomega;
            try {
                xomega = (Optional<Long>) input.get(input.size()-1).get();
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
                input.get(input.size()-1).get();
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
                toXomega = (Long) input.get(input.size()-1).get();
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
                input.get(input.size()-1).get();
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
        return Objects.toStringHelper(this).toString();
    }

    protected static final class CallSnapshot extends SimpleToStringListenableFuture<Long> {
        public static CallSnapshot create(CallSnapshotChain chain) {
            return create(
                    ChainedFutures.run(
                            ChainedFutures.process(
                                    ChainedFutures.chain(
                                            chain, 
                                            Lists.<ListenableFuture<?>>newArrayListWithCapacity(3)),
                                    ChainedFutures.<Long>castLast()),
                            SettableFuturePromise.<Long>create()));
        }

        public static CallSnapshot create(ListenableFuture<Long> future) {
            return new CallSnapshot(future);
        }
        
        protected CallSnapshot(ListenableFuture<Long> delegate) {
            super(delegate);
        }
    }

    protected final class CallSnapshotChain implements ChainedProcessor<ListenableFuture<?>> {
        
        public CallSnapshotChain() {}
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                List<ListenableFuture<?>> input) {
            switch (input.size()) {
            case 0:
            {
                ListenableFuture<?> future;
                try {
                    future = doSnapshot.call();
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            case 1:
            {
                Long xomega;
                try {
                    xomega = (Long) input.get(input.size()-1).get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.of(
                            undoSnapshot(undoSnapshot));
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
                    last = input.get(input.size()-1).get();
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
                CallablePromiseTask<Call<O>,Optional<Long>> task = CallablePromiseTask.create(
                        new Call<O>(cached, query), 
                        SettableFuturePromise.<Optional<Long>>create());
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
                final Materializer<StorageZNode<?>,?> materializer) {
            return Create.create(volume, materializer);
        }
        
        protected CreateXomega(ListenableFuture<KeeperException.Code> future) {
            super(future);
        }
        
        protected static class Create implements AsyncFunction<Long,KeeperException.Code> {

            public static Create create(
                    final VersionedId volume,
                    final Materializer<StorageZNode<?>,?> materializer) {
                return new Create(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Xomega.pathOf(volume.getValue(), volume.getVersion()), 
                        materializer);
            }
            
            private final ZNodePath path;
            private final Materializer<StorageZNode<?>,?> materializer;

            protected Create(
                    ZNodePath path,
                    Materializer<StorageZNode<?>,?> materializer) {
                this.path = path;
                this.materializer = materializer;
            }
            
            @Override
            public ListenableFuture<KeeperException.Code> apply(final Long input) throws Exception {
                return Futures.transform(
                        materializer.create(path, input).call(),
                        new Function<Operation.ProtocolResponse<?>, KeeperException.Code>() {
                            @Override
                            public KeeperException.Code apply(Operation.ProtocolResponse<?> response) {
                                if (response.record() instanceof Operation.Error) {
                                    return ((Operation.Error) response.record()).error();
                                } else {
                                    return KeeperException.Code.OK;
                                }
                            }
                        }, SameThreadExecutor.getInstance());
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
                            CALLBACK,
                            SameThreadExecutor.getInstance()));
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
