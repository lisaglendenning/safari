package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ChainedFutures.ChainedProcessor;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.OperatorVolumeLogEntry;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationCoordinatorEntry;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;

public final class VolumesEntryCoordinator extends Watchers.StopServiceOnFailure<ZNodePath> {

    public static VolumesEntryCoordinator listen(
            final AsyncFunction<? super VersionedId, Boolean> isResident,
            final AsyncFunction<VolumeOperationCoordinatorEntry, Boolean> execute,
            final SchemaClientService<ControlZNode<?>,?> client,
            final Service service) {
        final VolumesEntryCoordinator instance = new VolumesEntryCoordinator(
                isResident, 
                execute,
                client.materializer(),
                service);
        Watchers.CacheNodeCreatedListener.listen(
                client.materializer().cache(), 
                service, 
                client.cacheEvents(), 
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(instance), 
                        WatchMatcher.exact(
                                ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote.PATH, 
                                Watcher.Event.EventType.NodeCreated), 
                        instance.logger()), 
                instance.logger());
        return instance;
    }
    
    protected final Logger logger;
    protected final AsyncFunction<? super VersionedId, Boolean> isResident;
    protected final AsyncFunction<VolumeOperationCoordinatorEntry, Boolean> execute;
    protected final ConcurrentMap<AbsoluteZNodePath, VolumeEntryCoordinatorListener> coordinators;
    protected final Materializer<ControlZNode<?>,?> materializer;
    
    protected VolumesEntryCoordinator(
            final AsyncFunction<? super VersionedId, Boolean> isResident,
            final AsyncFunction<VolumeOperationCoordinatorEntry, Boolean> execute,
            final Materializer<ControlZNode<?>,?> materializer,
            Service service) {
        super(service);
        this.logger = LogManager.getLogger();
        this.isResident = isResident;
        this.execute = execute;
        this.materializer = materializer;
        this.coordinators = new MapMaker().makeMap();
    }
    
    public Logger logger() {
        return logger;
    }

    /**
     * Assumes cache is read locked.
     */
    @Override
    public void onSuccess(final ZNodePath result) {
        ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote vote = (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote) materializer.cache().cache().get(result);
        if (vote != null) {
            ControlSchema.Safari.Volumes.Volume.Log.Version.Entry entry = vote.entry();
            if ((entry.data().get() instanceof OperatorVolumeLogEntry) && !coordinators.containsKey(entry.path())) {
                final VersionedId id = entry.version().id();
                ListenableFuture<Boolean> resident;
                try {
                    resident = isResident.apply(id);
                    if (resident.isDone()) {
                        if (resident.get().booleanValue()) {
                            final VolumeOperation<?> operation = 
                                    VolumeOperation.valueOf(
                                            id,
                                            ((OperatorVolumeLogEntry) entry.data().get()).get());
                            LoggingFutureListener.listen(logger, 
                                    new VolumeEntryCoordinatorListener(
                                            new VolumeEntryCoordinator(
                                                    VolumeOperationCoordinatorEntry.existingEntry(
                                                            operation, entry.entryPath()),
                                                    execute,
                                                    materializer)));
                        }
                    } else {
                        new Callback(result, resident).run();
                    }
                } catch (Exception e) {
                    onFailure(e);
                }
            }
        }
    }
    
    private final class Callback extends SimpleToStringListenableFuture<Boolean> implements Runnable {

        private final ZNodePath path;
        
        private Callback(
                ZNodePath path, ListenableFuture<Boolean> delegate) {
            super(delegate);
            this.path = path;
        }

        @Override
        public void run() {
            if (isDone()) {
                Boolean result;
                try {
                    result = get();
                } catch (Exception e) {
                    onFailure(e);
                    return;
                }
                if (result.booleanValue()) {
                    materializer.cache().lock().readLock().lock();
                    try {
                        onSuccess(path);
                    } finally {
                        materializer.cache().lock().readLock().unlock();
                    }
                }
            } else {
                addListener(this, SameThreadExecutor.getInstance());
            }
        }
    }

    protected static final class VolumeEntryCoordinator extends ToStringListenableFuture<VolumeLogEntryPath> implements ChainedProcessor<ListenableFuture<?>> {

        public static VolumeEntryCoordinator create(
                VolumeOperationCoordinatorEntry entry,
                AsyncFunction<VolumeOperationCoordinatorEntry, Boolean> execute,
                Materializer<ControlZNode<?>,?> materializer) {
            return new VolumeEntryCoordinator(entry, execute, materializer);
        }
        
        private final VolumeOperationCoordinatorEntry delegate;
        private final AsyncFunction<VolumeOperationCoordinatorEntry, Boolean> execute;
        private final Materializer<ControlZNode<?>,?> materializer;
        
        protected VolumeEntryCoordinator(
                VolumeOperationCoordinatorEntry delegate,
                AsyncFunction<VolumeOperationCoordinatorEntry, Boolean> execute,
                Materializer<ControlZNode<?>,?> materializer) {
            this.delegate = delegate;
            this.execute = execute;
            this.materializer = materializer;
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(List<ListenableFuture<?>> input) throws Exception {
            if (input.isEmpty()) {
                return Optional.of(delegate());
            }
            ListenableFuture<?> last = input.get(input.size()-1);
            if (last instanceof VolumeOperationCoordinatorEntry) {
                VolumeLogEntryPath entry;
                try {
                    entry = ((VolumeOperationCoordinatorEntry) last).get();
                } catch (ExecutionException e) {
                    return Optional.of(Futures.immediateFailedFuture(e));
                }
                // check if operation is already committed
                ListenableFuture<?> future = null;
                materializer.cache().lock().readLock().lock();
                try {
                    ControlZNode<?> node = materializer.cache().cache().get(entry.path().join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.LABEL));
                    if ((node != null) && (node.data().stamp() > 0L)) {
                        future = Futures.immediateFuture(
                                        node.data().get());
                    }
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
                if (future == null) {
                    future = SubmittedRequests.submit(
                                    materializer,
                                    VolumesSchemaRequests.create(materializer)
                                        .version(entry.volume())
                                        .entry(entry.entry())
                                        .commit()
                                        .get());
                }
                return Optional.of(future);
            } else if (last instanceof SubmittedRequests) {
                SubmittedRequests<?,?> pending = (SubmittedRequests<?,?>) last;
                Optional<Operation.Error> error = null;
                try {
                    for (Operation.ProtocolResponse<?> response: pending.get()) {
                        error = Operations.maybeError(response.record(), 
                                KeeperException.Code.NONODE);
                    }
                } catch (Exception e) {
                    return Optional.of(Futures.immediateFailedFuture(e));
                }
                if (error.isPresent()) {
                    // proceed with operation
                    return Optional.of(execute.apply(delegate()));
                } else {
                    // already committed
                    assert (delegate().isDone());
                    VolumeLogEntryPath path = Futures.getUnchecked(delegate());
                    materializer.cache().lock().readLock().lock();
                    try {
                        return Optional.of(
                                Futures.immediateFuture(
                                        materializer.cache().cache().get(
                                                path.path().join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.LABEL))
                                                .data().get()));
                    } finally {
                        materializer.cache().lock().readLock().unlock();
                    }
                }
            } else {
                return Optional.absent();
            }
        }

        @Override
        protected VolumeOperationCoordinatorEntry delegate() {
            return delegate;
        }
    }
    
    protected final class VolumeEntryCoordinatorListener extends ToStringListenableFuture<Boolean> implements Runnable {

        protected final VolumeEntryCoordinator coordinator;
        protected final ChainedFutures.ChainedFuturesTask<?,?,Boolean> delegate;
        
        protected VolumeEntryCoordinatorListener(
                VolumeEntryCoordinator coordinator) {
            this.coordinator = coordinator;
            this.delegate = ChainedFutures.task(
                    ChainedFutures.process(
                        ChainedFutures.chain(
                                coordinator, 
                                Lists.<ListenableFuture<?>>newArrayListWithCapacity(3)), 
                        ChainedFutures.<Boolean>castLast()), 
                    SettableFuturePromise.<Boolean>create());
            addListener(this, SameThreadExecutor.getInstance());
            coordinator.addListener(this, SameThreadExecutor.getInstance());
        }
        
        @Override
        public synchronized void run() {
            if (isDone()) {
                if (coordinator.isDone() && !coordinator.isCancelled()) {
                    try {
                        coordinators.remove(coordinator.get().path(), this);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                    }
                }
            } else if (coordinator.isDone()) {
                if (coordinator.isCancelled()) {
                    cancel(true);
                    return;
                }
                
                AbsoluteZNodePath path;
                try {
                    path = coordinator.get().path();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    cancel(true);
                    return;
                }
                
                VolumeEntryCoordinatorListener existing = coordinators.putIfAbsent(path, this);
                if (existing == null) {
                    delegate().run();
                } else if (existing != this) {
                    cancel(true);
                }
            }
        }

        @Override
        protected ChainedFutures.ChainedFuturesTask<?,?,Boolean> delegate() {
            return delegate;
        }
        
        @Override
        protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper helper) {
            return super.toStringHelper(helper.addValue(coordinator));
        }
    }
}
