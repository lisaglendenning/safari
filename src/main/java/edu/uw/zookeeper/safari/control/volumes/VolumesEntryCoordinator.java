package edu.uw.zookeeper.safari.control.volumes;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.OperatorVolumeLogEntry;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationCoordinatorEntry;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.region.Region;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

public final class VolumesEntryCoordinator extends Watchers.StopServiceOnFailure<ZNodePath> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}
        
        @Provides @Singleton
        public VolumesEntryCoordinator getVolumesEntryCoordinator(
                final @Region AsyncFunction<VersionedId, Boolean> isResident,
                final TaskExecutor<VolumeOperationDirective,Boolean> operations,
                final @Volumes AsyncFunction<VersionedId, VolumeVersion<?>> branches,
                final SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> client,
                final RegionRoleService service) {
            return VolumesEntryCoordinator.listen(
                    isResident, 
                    new AsyncFunction<VolumeOperationCoordinatorEntry, Boolean>() {
                        @Override
                        public ListenableFuture<Boolean> apply(
                                VolumeOperationCoordinatorEntry input)
                                throws Exception {
                            return VolumeOperationCoordinator.forEntry(
                                            input, 
                                            operations,
                                            branches, 
                                            service, 
                                            client);
                        }
                    },  
                    client,
                    service);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(VolumesEntryCoordinator.class);
        }

        @Override
        protected void configure() {
        }
    }
    
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
                addListener(this, MoreExecutors.directExecutor());
            }
        }
    }

    protected static final class VolumeEntryCoordinator extends ToStringListenableFuture<VolumeLogEntryPath> implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain<ListenableFuture<?>>> {

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
        public Optional<? extends ListenableFuture<?>> apply(FutureChain<ListenableFuture<?>> input) throws Exception {
            if (input.isEmpty()) {
                return Optional.of(delegate());
            }
            ListenableFuture<?> last = input.getLast();
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
        protected final ChainedFutures.ChainedFuturesTask<Boolean> delegate;
        
        protected VolumeEntryCoordinatorListener(
                VolumeEntryCoordinator coordinator) {
            this.coordinator = coordinator;
            this.delegate = ChainedFutures.task(
                    ChainedFutures.<Boolean>castLast(
                        ChainedFutures.arrayList(
                                coordinator, 
                                3)));
            addListener(this, MoreExecutors.directExecutor());
            coordinator.addListener(this, MoreExecutors.directExecutor());
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
        protected ChainedFutures.ChainedFuturesTask<Boolean> delegate() {
            return delegate;
        }
        
        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
            return super.toStringHelper(helper.addValue(coordinator));
        }
    }
}
