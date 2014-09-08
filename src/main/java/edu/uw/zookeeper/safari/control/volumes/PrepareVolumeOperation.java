package edu.uw.zookeeper.safari.control.volumes;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ChainedFutures.ChainedProcessor;
import edu.uw.zookeeper.common.ForwardingPromise.SimpleForwardingPromise;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.AvailableIdentifier;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.LookupEntity;
import edu.uw.zookeeper.safari.schema.volumes.AssignParameters;
import edu.uw.zookeeper.safari.schema.volumes.BoundVolumeOperator;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.RegionAndLeaves;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeDescriptor;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperatorParameters;

public final class PrepareVolumeOperation<O extends Operation.ProtocolResponse<?>> implements ChainedProcessor<ListenableFuture<?>> {

    public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<VolumeOperation<?>> create(
            VolumesSchemaRequests<O>.VolumeSchemaRequests volume,
            VolumeOperator operator,
            List<?> arguments) {
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(
                                new PrepareVolumeOperation<O>(volume, operator, arguments),
                                Lists.<ListenableFuture<?>>newLinkedList()),
                        ChainedFutures.<VolumeOperation<?>>castLast()), 
                SettableFuturePromise.<VolumeOperation<?>>create());
    }
    
    protected final Logger logger;
    protected final VolumesSchemaRequests<O>.VolumeSchemaRequests volume;
    protected final VolumeOperator operator;
    protected final List<?> arguments;
    
    protected PrepareVolumeOperation(
            VolumesSchemaRequests<O>.VolumeSchemaRequests volume,
            VolumeOperator operator,
            List<?> arguments) {
        this.logger = LogManager.getLogger(this);
        this.volume = volume;
        this.operator = operator;
        this.arguments = arguments;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Optional<? extends ListenableFuture<?>> apply(List<ListenableFuture<?>> input) {
        if (input.isEmpty()) {
            return Optional.of(
                    LoggingFutureListener.listen(
                            logger, 
                            LookupLatestVersion.create(volume)));
        }
        
        Object last;
        try {
            last = input.get(input.size() - 1).get();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            return Optional.absent();
        }
        
        if (last instanceof VolumeOperation) {
            return Optional.absent();
        }
        
        UnsignedLong latest = (input.size() > 1) ? 
                (UnsignedLong) Futures.getUnchecked(input.get(0)) : 
                    (UnsignedLong) last;
                
        VolumeOperatorParameters parameters;
        switch (operator) {
        case MERGE:
        {
            // if we didn't find our parent the first time, try again
            if ((input.size() == 1) || !((Optional<VersionedId>) last).isPresent()) {
                return Optional.of(
                        LookupParent.create(volume));
            }
            VersionedId parent = ((Optional<VersionedId>) last).get();
            UnsignedLong version = UnsignedLong.valueOf(
                    Math.max(System.currentTimeMillis(),
                            Math.max(latest.longValue(),
                                    parent.getVersion().longValue())
                                    +1L));
            parameters = MergeParameters.create(parent, version);
            break;
        }
        case SPLIT:
        {
            ZNodeName branch = (ZNodeName) arguments.get(1);
            Optional<Identifier> leaf = ((input.size() > 1) && (last instanceof Identifier)) ?
                    Optional.of((Identifier) last): Optional.<Identifier>absent();
            if (!leaf.isPresent()) {
                ListenableFuture<?> future;
                volume.volumes().getMaterializer().cache().lock().readLock().lock();
                try {
                    ControlSchema.Safari.Volumes.Volume.Path path = (ControlSchema.Safari.Volumes.Volume.Path) volume.volumes().getMaterializer().cache().cache().get(ControlSchema.Safari.Volumes.Volume.Path.pathOf(volume.getVolume()));
                    if ((path == null) || (path.data().stamp() < 0L)) {
                        future = SubmittedRequests.submit(
                                        volume.volumes().getMaterializer(),
                                        volume.path().get());
                    } else {
                        future = AvailableIdentifier.sync(
                                path.data().get().join(branch),
                                ControlSchema.Safari.Volumes.class,
                                volume.volumes().getMaterializer());
                    }
                } finally {
                    volume.volumes().getMaterializer().cache().lock().readLock().unlock();
                }
                return Optional.of(future);
            }
            Identifier region = (Identifier) arguments.get(0);
            UnsignedLong version = UnsignedLong.valueOf(
                    Math.max(System.currentTimeMillis(),
                            latest.longValue()+1L));
            parameters = SplitParameters.create(branch, leaf.get(), region, version);
            break;
        }
        case TRANSFER:
        {
            Identifier region = (Identifier) arguments.get(0);
            UnsignedLong version = UnsignedLong.valueOf(
                    Math.max(System.currentTimeMillis(),
                            latest.longValue()+1L));
            parameters = AssignParameters.create(region, version);
            break;
        }
        default:
            throw new AssertionError();
        }
        return Optional.of(Futures.immediateFuture(
                VolumeOperation.valueOf(
                        VersionedId.valueOf(latest, volume.getVolume()),
                        BoundVolumeOperator.valueOf(operator, parameters))));
    }
    
    public static final class LookupLatestVersion<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> implements Callable<Optional<UnsignedLong>> {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<UnsignedLong> create(
                VolumesSchemaRequests<O>.VolumeSchemaRequests volume) {
            final SubmittedRequests<Records.Request,O> requests = SubmittedRequests.submit(
                    volume.volumes().getMaterializer(),
                    volume.latest().get());
            CallablePromiseTask<LookupLatestVersion<O>,UnsignedLong> task = 
                    CallablePromiseTask.listen(
                            new LookupLatestVersion<O>(volume, requests), 
                            SettableFuturePromise.<UnsignedLong>create());
            return task;
        }
        
        private final VolumesSchemaRequests<?>.VolumeSchemaRequests volume;
        
        protected LookupLatestVersion(
                VolumesSchemaRequests<?>.VolumeSchemaRequests volume,
                ListenableFuture<List<O>> future) {
            super(future);
            this.volume = volume;
        }
        
        @Override
        public Optional<UnsignedLong> call() throws Exception {
            if (isDone()) {
                for (Operation.ProtocolResponse<?> response: get()) {
                    Operations.unlessError(response.record());
                }
                UnsignedLong latest;
                volume.volumes().getMaterializer().cache().lock().readLock().lock();
                try {
                    ControlSchema.Safari.Volumes.Volume.Log.Latest node = 
                            (ControlSchema.Safari.Volumes.Volume.Log.Latest) volume.volumes().getMaterializer().cache().cache().get(volume.latest().getPath());
                    latest = (node != null) ? (UnsignedLong) node.data().get() : null;
                    if (latest == null) {
                        throw new KeeperException.BadArgumentsException();
                    }
                } finally {
                    volume.volumes().getMaterializer().cache().lock().readLock().unlock();
                }
                return Optional.of(latest);
            }
            return Optional.absent();
        }
        
        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper toString) {
            return super.toStringHelper(toString.addValue(volume.getVolume()));
        }
    }
    
    public static final class LookupParent implements ChainedProcessor<ListenableFuture<?>> {
        
        public static ListenableFuture<Optional<VersionedId>> create(
                VolumesSchemaRequests<?>.VolumeSchemaRequests volume) {
            return ChainedFutures.run(
                    ChainedFutures.process(
                            ChainedFutures.chain(
                                    new LookupParent(volume),
                                    Lists.<ListenableFuture<?>>newLinkedList()),
                            ChainedFutures.<Optional<VersionedId>>castLast()),
                    SettableFuturePromise.<Optional<VersionedId>>create());
        }
        
        protected final VolumesSchemaRequests<?>.VolumeSchemaRequests volume;
        
        protected LookupParent(VolumesSchemaRequests<?>.VolumeSchemaRequests volume) {
            this.volume = volume;
        }
    
        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                List<ListenableFuture<?>> input) throws Exception {
            Optional<? extends ListenableFuture<?>> last = input.isEmpty() ? Optional.<ListenableFuture<?>>absent() : Optional.of(input.get(input.size()-1));
            if (last.isPresent() && (last.get() instanceof LookupPrefixes)) {
                return Optional.absent();
            }
            
            final Optional<ZNodePath> path;
            volume.volumes().getMaterializer().cache().lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes.Volume.Path node = 
                        (ControlSchema.Safari.Volumes.Volume.Path) volume.volumes().getMaterializer().cache().cache().get(volume.path().getPath());
                if ((node == null) || (node.data().stamp() < 0L)) {
                    path = Optional.absent();
                } else {
                    path = Optional.of(node.data().get());
                }
            } finally {
                volume.volumes().getMaterializer().cache().lock().readLock().unlock();
            }
            if (!path.isPresent()) {
                return Optional.of(SubmittedRequests.submit(volume.volumes().getMaterializer(), volume.path().get()));
            }
            
            if (!last.isPresent() || ((Records.PathGetter) ((SubmittedRequests<Records.Request,?>) last.get()).requests().get(0)).getPath().equals(volume.path().getPath().toString())) {
                return Optional.of(SubmittedRequests.submit(volume.volumes().getMaterializer(), volume.volumes().children()));
            }
            
            return Optional.of(LookupPrefixes.create(VolumeDescriptor.valueOf(volume.getVolume(), path.get()), volume.volumes()));
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(volume.getVolume()).toString();
        }
    }

    public static final class LookupPrefixes extends SimpleForwardingPromise<Optional<VersionedId>> implements Runnable {
        
        public static ListenableFuture<Optional<VersionedId>> create(
                VolumeDescriptor volume,
                VolumesSchemaRequests<?> schema) {
            checkArgument(volume.getPath() instanceof AbsoluteZNodePath);
            return new LookupPrefixes(volume, schema,
                    SettableFuturePromise.<Optional<VersionedId>>create());
        }
        
        protected final Logger logger;
        protected final List<Callback> lookups;
        
        protected LookupPrefixes(
                VolumeDescriptor volume,
                VolumesSchemaRequests<?> schema,
                Promise<Optional<VersionedId>> promise) {
            super(promise);
            this.logger = LogManager.getLogger(this);
            ImmutableList.Builder<Callback> lookups = ImmutableList.builder();
            ZNodePath prefix = ZNodePath.root();
            for (Iterator<ZNodeLabel> labels = volume.getPath().iterator();
                    labels.hasNext(); prefix = prefix.join(labels.next())) {
                lookups.add(
                        LoggingFutureListener.listen(
                            logger, 
                            Callback.create(
                                promise,
                                LookupPrefix.create(
                                        volume.getId(), 
                                        prefix, 
                                        schema))));
            }
            this.lookups = lookups.build();
            Futures.successfulAsList(this.lookups).addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public void run() {
            if (!isDone()) {
                for (Callback lookup: lookups) {
                    if (!lookup.isDone()) {
                        return;
                    }
                }
                // all lookups failed
                set(Optional.<VersionedId>absent());
            }
        }
        
        protected static class Callback extends SimpleToStringListenableFuture<Optional<VersionedId>> implements Runnable {
    
            public static Callback create(
                    Promise<Optional<VersionedId>> promise,
                    ListenableFuture<Optional<VersionedId>> future) {
                return new Callback(promise, future);
            }
            
            protected final Promise<Optional<VersionedId>> promise;
            
            protected Callback(
                    Promise<Optional<VersionedId>> promise,
                    ListenableFuture<Optional<VersionedId>> future) {
                super(future);
                this.promise = promise;
                addListener(this, MoreExecutors.directExecutor());
                promise.addListener(this, MoreExecutors.directExecutor());
            }
            
            @Override
            public void run() {
                if (isDone()) {
                    if (!isCancelled()) {
                        Optional<VersionedId> result;
                        try {
                            result = get();
                            if (result.isPresent()) {
                                promise.set(result);
                            }
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        } catch (ExecutionException e) {
                            promise.setException(e);
                        }
                    }
                } else if (promise.isDone()) {
                    cancel(false);
                }
            }
        }
    }

    public static final class LookupPrefix implements ChainedProcessor<ListenableFuture<?>> {
    
        public static ListenableFuture<Optional<VersionedId>> create(
                Identifier child,
                ZNodePath prefix,
                VolumesSchemaRequests<?> schema) {
            return ChainedFutures.run(
                    ChainedFutures.process(
                            ChainedFutures.chain(
                                    new LookupPrefix(child, prefix, schema), 
                                    Lists.<ListenableFuture<?>>newLinkedList()), 
                            ChainedFutures.<Optional<VersionedId>>castLast()),
                    SettableFuturePromise.<Optional<VersionedId>>create());
        }
        
        protected final Logger logger;
        protected final Identifier child;
        protected final ZNodePath prefix;
        protected final VolumesSchemaRequests<?> schema;
        
        protected LookupPrefix(
                Identifier child,
                ZNodePath prefix,
                VolumesSchemaRequests<?> schema) {
            this.logger = LogManager.getLogger(this);
            this.child = child;
            this.prefix = prefix;
            this.schema = schema; 
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                List<ListenableFuture<?>> input) throws Exception {
            if (input.isEmpty()) {
                ListenableFuture<?> future;
                try {
                    future = LookupEntity.sync(
                                    prefix, 
                                    ControlSchema.Safari.Volumes.class, 
                                    schema.getMaterializer());
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            
            Optional<Identifier> entity = (Optional<Identifier>) input.get(0).get();
            
            if (input.size() == 1) {
                final ListenableFuture<?> future;
                if (entity.isPresent()) {
                    future = LoggingFutureListener.listen(
                                    logger, 
                                    LookupLatestVersion.create(
                                            schema.volume(entity.get())));
                } else {
                    future = Futures.immediateFuture(
                                    Optional.<VersionedId>absent());
                }
                return Optional.of(future);
            }
            
            Object last = input.get(input.size()-1).get();
            
            if (last instanceof Optional) {
                return Optional.absent();
            } else {
                final UnsignedLong version = (UnsignedLong) last;
                return Optional.of(
                        LoggingFutureListener.listen(
                                logger, 
                                Callback.create(
                                        child,
                                        (VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests) schema.volume(entity.get()).version(version))));
            }
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("prefix", prefix).add("child", child).toString();
        }
        
        protected static class Callback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Optional<VersionedId>> {
    
            public static ListenableFuture<Optional<VersionedId>> create(
                    Identifier child,
                    VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests schema) {
                try {
                    return new Callback(child, schema).apply(ImmutableList.<Operation.ProtocolResponse<?>>of());
                } catch (Exception e) {
                    return Futures.immediateFailedFuture(e);
                }
            }
            
            protected final Identifier child;
            protected final VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests schema;
            
            protected Callback(
                    Identifier child,
                    VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests schema) {
                this.child = child;
                this.schema = schema;
            }
            
            @Override
            public ListenableFuture<Optional<VersionedId>> apply(
                    List<? extends Operation.ProtocolResponse<?>> input)
                    throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    Operations.unlessError(response.record());
                }
                schema.volume().volumes().getMaterializer().cache().lock().readLock().lock();
                try {
                    ControlSchema.Safari.Volumes.Volume.Log.Version node = 
                            (ControlSchema.Safari.Volumes.Volume.Log.Version) schema.volume().volumes().getMaterializer().cache().cache().get(schema.getPath());
                    if ((node != null) && (node.state() != null) && (node.state().data().stamp() > 0L)) {
                        final Optional<RegionAndLeaves> state = Optional.fromNullable(node.state().data().get());
                        Optional<VersionedId> value;
                        if (state.isPresent() && state.get().getLeaves().contains(child)) {
                            value = Optional.of(VersionedId.valueOf(schema.getVersion(), schema.volume().getVolume()));
                        } else {
                            value = Optional.<VersionedId>absent();
                        }
                        return Futures.immediateFuture(value);
                    }
                } finally {
                    schema.volume().volumes().getMaterializer().cache().lock().readLock().unlock();
                }
                return Futures.transform(
                        SubmittedRequests.submit(
                                schema.volume().volumes().getMaterializer(), 
                                schema.state().get()),
                        this);
            }
        }
    }
}