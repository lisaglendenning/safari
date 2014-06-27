package edu.uw.zookeeper.safari.data;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
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
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.volume.AssignParameters;
import edu.uw.zookeeper.safari.volume.BoundVolumeOperator;
import edu.uw.zookeeper.safari.volume.MergeParameters;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;
import edu.uw.zookeeper.safari.volume.SplitParameters;
import edu.uw.zookeeper.safari.volume.VolumeDescriptor;
import edu.uw.zookeeper.safari.volume.VolumeOperation;
import edu.uw.zookeeper.safari.volume.VolumeOperator;
import edu.uw.zookeeper.safari.volume.VolumeOperatorParameters;

public class PrepareVolumeOperation implements Function<List<ListenableFuture<?>>,Optional<? extends ListenableFuture<?>>> {

    public static ListenableFuture<VolumeOperation<?>> create(
            VolumesSchemaRequests<?>.VolumeSchemaRequests volume,
            VolumeOperator operator,
            List<?> arguments,
            Promise<VolumeOperation<?>> promise) {
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(
                                new PrepareVolumeOperation(volume, operator, arguments),
                                Lists.<ListenableFuture<?>>newLinkedList()),
                        ChainedFutures.<VolumeOperation<?>>castLast()), 
                promise);
    }
    
    protected final Logger logger;
    protected final VolumesSchemaRequests<?>.VolumeSchemaRequests volume;
    protected final VolumeOperator operator;
    protected final List<?> arguments;
    
    protected PrepareVolumeOperation(
            VolumesSchemaRequests<?>.VolumeSchemaRequests volume,
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
            ListenableFuture<?> future = LookupLatestVersion.create(
                    volume, 
                    SettableFuturePromise.<UnsignedLong>create());
            LoggingFutureListener.listen(logger, future);
            return Optional.of(future);
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
    
    public static class LookupLatestVersion extends ForwardingListenableFuture<List<? extends Operation.ProtocolResponse<?>>> implements Callable<Optional<UnsignedLong>> {

        public static ListenableFuture<UnsignedLong> create(
                VolumesSchemaRequests<?>.VolumeSchemaRequests volume,
                Promise<UnsignedLong> promise) {
            CallablePromiseTask<LookupLatestVersion,UnsignedLong> task = CallablePromiseTask.create(
                    new LookupLatestVersion(volume), promise);
            task.task().addListener(task, SameThreadExecutor.getInstance());
            return task;
        }
        protected final VolumesSchemaRequests<?>.VolumeSchemaRequests volume;
        protected final SubmittedRequests<Records.Request,?> future;
        
        protected LookupLatestVersion(
                VolumesSchemaRequests<?>.VolumeSchemaRequests volume) {
            this.volume = volume;
            this.future = SubmittedRequests.submit(
                    volume.volumes().getMaterializer(),
                    volume.latest().get());
        }
        
        public VolumesSchemaRequests<?>.VolumeSchemaRequests volume() {
            return volume;
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
        
        @SuppressWarnings("unchecked")
        @Override
        protected ListenableFuture<List<? extends Operation.ProtocolResponse<?>>> delegate() {
            return (ListenableFuture<List<? extends Operation.ProtocolResponse<?>>>) (ListenableFuture<?>) future;
        }
    }
    
    public static class LookupParent implements Function<List<ListenableFuture<?>>,Optional<? extends ListenableFuture<?>>> {
        
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
                List<ListenableFuture<?>> input) {
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
            
            return Optional.of(LookupPrefixes.create(VolumeDescriptor.valueOf(volume.getVolume(), path.get()), volume.volumes(), SettableFuturePromise.<Optional<VersionedId>>create()));
        }
        
        protected static class LookupPrefix implements Function<List<ListenableFuture<?>>, Optional<? extends ListenableFuture<?>>> {

            public static ListenableFuture<Optional<VersionedId>> create(
                    Identifier child,
                    ZNodePath prefix,
                    VolumesSchemaRequests<?> schema,
                    Promise<Optional<VersionedId>> promise) {
                return ChainedFutures.run(
                        ChainedFutures.process(
                                ChainedFutures.chain(
                                        new LookupPrefix(child, prefix, schema), 
                                        Lists.<ListenableFuture<?>>newLinkedList()), 
                                ChainedFutures.<Optional<VersionedId>>castLast()), 
                        promise);
            }
            
            protected final Logger logger;
            protected final Identifier child;
            protected final ZNodePath prefix;
            protected final VolumesSchemaRequests<?> schema;
            
            public LookupPrefix(
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
                    List<ListenableFuture<?>> input) {
                if (input.isEmpty()) {
                    try {
                        return Optional.of(LookupEntity.create(prefix, ControlSchema.Safari.Volumes.class, schema.getMaterializer()).call());
                    } catch (Exception e) {
                        return Optional.of(Futures.immediateFailedFuture(e));
                    }
                }
                
                Optional<Identifier> entity;
                try {
                    entity = (Optional<Identifier>) input.get(0).get();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
                
                if (input.size() == 1) {
                    if (entity.isPresent()) {
                        ListenableFuture<?> future = 
                                LookupLatestVersion.create(
                                        schema.volume(entity.get()),
                                        SettableFuturePromise.<UnsignedLong>create());
                        LoggingFutureListener.listen(logger, future);
                        return Optional.of(future);
                    } else {
                        return Optional.of(Futures.immediateFuture(Optional.<VersionedId>absent()));
                    }
                }
                
                Object last;
                try {
                    last = input.get(1).get();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
                
                if (last instanceof Optional) {
                    return Optional.absent();
                }
                
                try {
                    return Optional.of(
                            new Callback(
                                    child,
                                    (VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests) schema.volume(entity.get()).version((UnsignedLong) last))
                                .apply(ImmutableList.<Operation.ProtocolResponse<?>>of()));
                } catch (Exception e) {
                    return Optional.of(Futures.immediateFailedFuture(e));
                }
            }
            
            protected static class Callback implements AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Optional<VersionedId>> {

                protected final Identifier child;
                protected final VolumesSchemaRequests<?>.VolumeSchemaRequests.VolumeVersionSchemaRequests schema;
                
                public Callback(
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
                            Optional<RegionAndLeaves> state = Optional.fromNullable(node.state().data().get());
                            if (state.isPresent() && state.get().getLeaves().contains(child)) {
                                return Futures.immediateFuture(Optional.of(VersionedId.valueOf(schema.getVersion(), schema.volume().getVolume())));
                            } else {
                                return Futures.immediateFuture(Optional.<VersionedId>absent());
                            }
                        }
                    } finally {
                        schema.volume().volumes().getMaterializer().cache().lock().readLock().unlock();
                    }
                    return Futures.transform(
                            SubmittedRequests.submit(schema.volume().volumes().getMaterializer(), schema.state().get()),
                            this,
                            SameThreadExecutor.getInstance());
                }
            }
        }
        
        protected static class LookupPrefixes extends ForwardingListenableFuture<Optional<VersionedId>> implements Runnable {
            
            public static ListenableFuture<Optional<VersionedId>> create(
                    VolumeDescriptor volume,
                    VolumesSchemaRequests<?> schema,
                    Promise<Optional<VersionedId>> promise) {
                return new LookupPrefixes(volume, schema, promise);
            }
            
            protected final Logger logger;
            protected final List<Callback> lookups;
            protected final Promise<Optional<VersionedId>> promise;
            
            protected LookupPrefixes(
                    VolumeDescriptor volume,
                    VolumesSchemaRequests<?> schema,
                    Promise<Optional<VersionedId>> promise) {
                this.logger = LogManager.getLogger(this);
                this.promise = promise;
                ImmutableList.Builder<Callback> lookups = ImmutableList.builder();
                ZNodePath prefix = ZNodePath.root();
                Iterator<ZNodeLabel> labels = volume.getPath().iterator();
                while (labels.hasNext()) {
                    Callback callback = new Callback(LookupPrefix.create(volume.getId(), prefix, schema,
                                    SettableFuturePromise.<Optional<VersionedId>>create()), promise);
                    LoggingFutureListener.listen(logger, callback);
                    lookups.add(callback);
                    prefix = prefix.join(labels.next());
                }
                this.lookups = lookups.build();
                addListener(this, SameThreadExecutor.getInstance());
                Futures.allAsList(this.lookups).addListener(this, SameThreadExecutor.getInstance());
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
                    promise.set(Optional.<VersionedId>absent());
                } else {
                    for (Callback lookup: lookups) {
                        if (!lookup.isDone()) {
                            lookup.cancel(false);
                        }
                    }
                }
            }

            @Override
            protected ListenableFuture<Optional<VersionedId>> delegate() {
                return promise;
            }
            
            protected static class Callback extends ForwardingListenableFuture<Optional<VersionedId>> implements Runnable {

                protected final Promise<Optional<VersionedId>> promise;
                protected final ListenableFuture<Optional<VersionedId>> future;
                
                public Callback(
                        ListenableFuture<Optional<VersionedId>> future,
                        Promise<Optional<VersionedId>> promise) {
                    this.promise = promise;
                    this.future = future;
                    addListener(this, SameThreadExecutor.getInstance());
                }
                
                @Override
                public void run() {
                    if (isDone()) {
                        Optional<VersionedId> result;
                        try {
                            result = get();
                            if (result.isPresent()) {
                                promise.set(result);
                            }
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        } catch (ExecutionException e) {
                            promise.setException(e);
                        }
                    }
                }

                @Override
                protected ListenableFuture<Optional<VersionedId>> delegate() {
                    return future;
                }
            }
        }
    }
}