package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.OperatorVolumeLogEntry;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.volume.RegionAndBranches;
import edu.uw.zookeeper.safari.volume.RegionAndLeaves;
import edu.uw.zookeeper.safari.volume.VolumeOperation;

public class VolumesEntryCoordinator extends AbstractWatchListener {

    public static VolumesEntryCoordinator listen(
            Predicate<? super Identifier> isAssigned,
            final TaskExecutor<VolumeOperationDirective,Boolean> executor,
            final Function<? super Identifier, ZNodePath> paths,
            final Function<? super VersionedId, Optional<RegionAndBranches>> states,
            ControlClientService control,
            Service service) {
        VolumesEntryCoordinator listener = new VolumesEntryCoordinator(isAssigned, executor, paths, states, control, service);
        listener.listen();
        return listener;
    }
    
    protected final Logger logger;
    protected final ControlClientService control;
    protected final Predicate<? super Identifier> isAssigned;
    protected final ConcurrentMap<AbsoluteZNodePath, VolumeEntryCoordinatorListener> coordinators;
    protected final TaskExecutor<VolumeOperationDirective,Boolean> executor;
    protected final Function<? super Identifier, ZNodePath> paths;
    protected final Function<? super VersionedId, Optional<RegionAndBranches>> states;
    
    protected VolumesEntryCoordinator(
            Predicate<? super Identifier> isAssigned,
            final TaskExecutor<VolumeOperationDirective,Boolean> executor,
            final Function<? super Identifier, ZNodePath> paths,
            final Function<? super VersionedId, Optional<RegionAndBranches>> states,
            ControlClientService control,
            Service service) {
        super(service, control.cacheEvents(), 
                WatchMatcher.exact(
                        ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote.PATH, 
                        Watcher.Event.EventType.NodeCreated, 
                        Watcher.Event.EventType.NodeDataChanged));
        this.logger = LogManager.getLogger();
        this.isAssigned = isAssigned;
        this.control = control;
        this.executor = executor;
        this.paths = paths;
        this.states = states;
        this.coordinators = new MapMaker().makeMap();
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        final ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote node = (ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Vote) control.materializer().cache().cache().get(event.getPath());
        if (node.entry().data().stamp() < 0L) {
            Watchers.ReplayEvent.forListener(this, event, 
                    WatchMatcher.exact(node.entry().path(), 
                            Watcher.Event.EventType.NodeDataChanged)).listen();
        } else {
            if ((node.entry().version().state() == null) || (node.entry().version().state().data().stamp() < 0L)) {
                Watchers.ReplayEvent.forListener(this, event,
                        WatchMatcher.exact(
                                ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(node.entry().version().log().volume().name(), node.entry().version().name()), 
                                Watcher.Event.EventType.NodeCreated,
                                Watcher.Event.EventType.NodeDataChanged)).listen();
            } else {
                final Optional<RegionAndLeaves> state =  Optional.fromNullable(node.entry().version().state().data().get());
                final boolean assigned = state.isPresent() && isAssigned.apply(state.get().getRegion());
                if (assigned && (node.entry().data().get() instanceof OperatorVolumeLogEntry)) {
                    final VolumeOperation<?> operation = 
                            VolumeOperation.valueOf(
                                    node.entry().version().id(),
                                    ((OperatorVolumeLogEntry) node.entry().data().get()).get());
                    final VolumeOperationCoordinatorEntry entry = VolumeOperationCoordinatorEntry.existingEntry(
                            operation, node.entry().entryPath());
                    coordinate(entry);
                }
            }
        }
    }
    
    protected VolumeEntryCoordinatorListener coordinate(VolumeOperationCoordinatorEntry entry) {
        return LoggingFutureListener.listen(logger, 
                        new VolumeEntryCoordinatorListener(
                                new VolumeEntryCoordinator(entry)));
    }

    protected class VolumeEntryCoordinator extends ForwardingListenableFuture<VolumeLogEntryPath> implements Function<List<ListenableFuture<?>>,Optional<? extends ListenableFuture<?>>> {

        protected final VolumeOperationCoordinatorEntry entry;
        
        public VolumeEntryCoordinator(
                VolumeOperationCoordinatorEntry entry) {
            this.entry = entry;
        }
        
        @Override
        public Optional<? extends ListenableFuture<?>> apply(List<ListenableFuture<?>> input) {
            if (input.isEmpty()) {
                // first, check if operation is already committed
                VolumeLogEntryPath path;
                try {
                    path = entry.get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (ExecutionException e) {
                    return Optional.of(Futures.immediateFailedFuture(e));
                }
                return Optional.of(
                        SubmittedRequests.submit(
                                control.materializer(),
                                VolumesSchemaRequests.create(control.materializer())
                                    .version(path.volume())
                                    .entry(path.entry())
                                    .commit()
                                    .get()));
            }
            
            ListenableFuture<?> last = input.get(input.size()-1);
            if (last instanceof SubmittedRequests) {
                SubmittedRequests<?,?> pending = (SubmittedRequests<?,?>) last;
                Optional<Operation.Error> error;
                try {
                    error = Operations.maybeError(pending.get().get(1).record(), 
                            KeeperException.Code.NONODE);
                } catch (Exception e) {
                    return Optional.of(Futures.immediateFailedFuture(e));
                }
                if (error.isPresent()) {
                    // proceed with operation
                    return Optional.of(
                            VolumeOperationCoordinator.forEntry(
                                    entry, 
                                    executor, 
                                    paths, 
                                    states, 
                                    service, 
                                    control));
                } else {
                    // already committed
                    assert (entry.isDone());
                    VolumeLogEntryPath path = Futures.getUnchecked(entry);
                    control.materializer().cache().lock().readLock().lock();
                    try {
                        return Optional.of(
                                Futures.immediateFuture(
                                        control.materializer().cache().cache().get(
                                                path.path().join(ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.Commit.LABEL))
                                                .data().get()));
                    } finally {
                        control.materializer().cache().lock().readLock().unlock();
                    }
                }
            } else {
                return Optional.absent();
            }
        }

        @Override
        protected VolumeOperationCoordinatorEntry delegate() {
            return entry;
        }
    }
    
    protected class VolumeEntryCoordinatorListener extends ForwardingListenableFuture<Boolean> implements Runnable {

        protected final VolumeEntryCoordinator coordinator;
        protected final ChainedFutures.ChainedFuturesTask<?,?,Boolean> delegate;
        
        public VolumeEntryCoordinatorListener(
                VolumeEntryCoordinator coordinator) {
            this.coordinator = coordinator;
            this.delegate = ChainedFutures.task(
                    ChainedFutures.process(
                        ChainedFutures.chain(
                                coordinator, 
                                Lists.<ListenableFuture<?>>newArrayListWithCapacity(2)), 
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
                AbsoluteZNodePath path;
                try {
                    path = coordinator.get().path();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                    cancel(true);
                    return;
                }
                
                VolumeEntryCoordinatorListener existing = coordinators.putIfAbsent(path, this);
                if ((existing == null) || (existing == this)) {
                    delegate().run();
                } else {
                    cancel(true);
                }
            }
        }

        @Override
        protected ChainedFutures.ChainedFuturesTask<?,?,Boolean> delegate() {
            return delegate;
        }
    }
}