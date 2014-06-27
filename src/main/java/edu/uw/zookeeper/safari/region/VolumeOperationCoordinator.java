package edu.uw.zookeeper.safari.region;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.schema.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.volume.MergeParameters;
import edu.uw.zookeeper.safari.volume.RegionAndBranches;
import edu.uw.zookeeper.safari.volume.SplitParameters;
import edu.uw.zookeeper.safari.volume.VolumeOperation;

public class VolumeOperationCoordinator extends ForwardingListenableFuture.SimpleForwardingListenableFuture<Boolean> {

    public static VolumeOperationCoordinator forEntry(
            final VolumeOperationCoordinatorEntry entry,
            final TaskExecutor<VolumeOperationDirective,Boolean> executor,
            final Function<? super Identifier, ZNodePath> paths,
            final Function<? super VersionedId, Optional<RegionAndBranches>> states,
            final Service service,
            final ControlClientService control) {
        ListenableFuture<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>> entries = VolumeOperationProposer.create(
                entry, control.materializer(),
                SettableFuturePromise.<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>>create());
        LoggingFutureListener.listen(LogManager.getLogger(VolumeOperationCoordinator.class), entries);
        VolumeOperationCoordinator instance = create(
                entry.operation(), 
                entries, 
                executor, 
                paths,
                states,
                service,
                control);
        LoggingFutureListener.listen(
                LogManager.getLogger(VolumeOperationCoordinator.class), instance);
        return instance;
    }
    
    public static VolumeOperationCoordinator create(
            final VolumeOperation<?> operation,
            final ListenableFuture<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>> entries,
            final TaskExecutor<VolumeOperationDirective,Boolean> executor,
            final Function<? super Identifier, ZNodePath> paths,
            final Function<? super VersionedId, Optional<RegionAndBranches>> states,
            final Service service,
            final ControlClientService control) {
        final Supplier<Propose> proposer = new Supplier<Propose>() {
            @Override
            public Propose get() {
                return Propose.create(
                        entries, 
                        control.materializer().cache(), 
                        service, 
                        control.cacheEvents(),
                        SettableFuturePromise.<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>>create());
            }
        };
        final Call call = new Call(
                operation, 
                proposer, 
                executor, 
                VolumeOperationRequests.create(
                        VolumesSchemaRequests.create(
                                control.materializer()), paths, states));
        final ListenableFuture<Boolean> future = ChainedFutures.run(
                ChainedFutures.process(
                    ChainedFutures.chain(
                            call,
                            Lists.<Action<?>>newArrayListWithCapacity(3)), 
                    ChainedFutures.<Boolean>castLast()),
                SettableFuturePromise.<Boolean>create());
        return new VolumeOperationCoordinator(call, future);
    }

    protected final Call call;
    
    protected VolumeOperationCoordinator(
            Call call, ListenableFuture<Boolean> delegate) {
        super(delegate);
        this.call = call;
    }
    
    public VolumeOperation<?> operation() {
        return call.operation();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("operation", operation()).add("future", ToStringListenableFuture.toString(delegate())).toString();
    }

    protected static class Call implements Function<List<VolumeOperationCoordinator.Action<?>>,Optional<? extends VolumeOperationCoordinator.Action<?>>> {

        protected final VolumeOperation<?> operation;
        protected final Supplier<Propose> proposer;
        protected final TaskExecutor<VolumeOperationDirective,Boolean> executor;
        protected final VolumeOperationRequests<?> requests;
        
        public Call(
                VolumeOperation<?> operation,
                Supplier<Propose> proposer,
                TaskExecutor<VolumeOperationDirective,Boolean> executor,
                VolumeOperationRequests<?> requests) {
            this.operation = operation;
            this.proposer = proposer;
            this.executor = executor;
            this.requests = requests;
        }
        
        public VolumeOperation<?> operation() {
            return operation;
        }
        
        @Override
        public Optional<? extends Action<?>> apply(List<Action<?>> input) {
            if (input.isEmpty()) {
                return Optional.of(propose());
            }
            Action<?> last = input.get(input.size() - 1);
            if (last instanceof Propose) {
                try {
                    return Optional.of(execute((Propose) last));
                } catch (Exception e) {
                    return Optional.absent();
                }
            } else if (last instanceof Execute) {
                try {
                    return Optional.of(commit((Propose) input.get(input.size() - 2), (Execute) last));
                } catch (Exception e) {
                    return Optional.absent();
                }
            } else {
                return Optional.absent();
            }
        }
        
        protected Propose propose() {
            return LoggingFutureListener.listen(
                            LogManager.getLogger(VolumeOperationCoordinator.class),
                            proposer.get());
        }
        
        protected Execute execute(Propose propose) throws InterruptedException, ExecutionException {
            VolumeLogEntryPath first = null;
            Boolean commit = Boolean.TRUE;
            for (Pair<VolumeLogEntryPath,Boolean> e: propose.get()) {
                if (first == null) {
                    first = e.first();
                }
                if (e.second().equals(Boolean.FALSE)) {
                    commit = Boolean.FALSE;
                    break;
                }
            }
            assert (first != null);
            return LoggingFutureListener.listen(
                    LogManager.getLogger(VolumeOperationCoordinator.class),
                    Execute.create(
                            VolumeOperationDirective.create(first, operation, commit),
                            executor));
        }
        
        protected Commit commit(Propose propose, Execute execute) throws InterruptedException, ExecutionException {
            Boolean commit = execute.get();
            ImmutableList<Pair<VolumeLogEntryPath, Boolean>> votes = propose.get();
            ImmutableList.Builder<VolumeLogEntryPath> entries = ImmutableList.builder();
            for (Pair<VolumeLogEntryPath, Boolean> vote: votes) {
                entries.add(vote.first());
            }
            return LoggingFutureListener.listen(
                    LogManager.getLogger(VolumeOperationCoordinator.class),
                    Commit.create(operation, entries.build(), commit, requests));
        }
    }
    
    public static abstract class Action<V> extends ToStringListenableFuture<V> {
        
        protected Action(ListenableFuture<V> future) {
            super(future);
        }
    }
    
    public static final class Propose extends Action<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> {

        public static Propose create(
                ListenableFuture<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>> entries,
                LockableZNodeCache<ControlZNode<?>,?,?> cache,
                Service service,
                WatchListeners watch,
                Promise<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> promise) {
            final Call call = new Call(entries, cache, service, watch);
            final ListenableFuture<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> future = ChainedFutures.run(
                    ChainedFutures.process(
                        ChainedFutures.chain(
                                call, 
                                Lists.<ListenableFuture<?>>newArrayListWithCapacity(2)),
                        ChainedFutures.<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>>castLast()),
                    promise);
            return new Propose(future);
        }
        
        protected Propose(
                ListenableFuture<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> future) {
            super(future);
        }

        protected static final class Call implements Function<List<ListenableFuture<?>>, Optional<? extends ListenableFuture<?>>> {

            protected final ListenableFuture<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>> entries;
            protected final LockableZNodeCache<ControlZNode<?>,?,?> cache;
            protected final Service service;
            protected final WatchListeners watch;
            
            public Call(
                    ListenableFuture<Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>>> entries,
                    LockableZNodeCache<ControlZNode<?>,?,?> cache,
                    Service service,
                    WatchListeners watch) {
                this.entries = entries;
                this.cache = cache;
                this.service = service;
                this.watch = watch;
            }
            
            @Override
            public Optional<? extends ListenableFuture<?>> apply(
                    List<ListenableFuture<?>> input) {
                if (input.isEmpty()) {
                    return Optional.of(entries);
                }
                if (input.get(input.size() - 1) == entries) {
                    final Pair<VolumeLogEntryPath,Set<VolumeLogEntryPath>> paths;
                    try {
                        paths = entries.get();
                    } catch (Exception e) {
                        return Optional.absent();
                    }
                    final Logger logger = LogManager.getLogger(VolumeOperationCoordinator.class);
                    final ImmutableList.Builder<VolumeEntryVoteListener> futures = ImmutableList.builder();
                    for (VolumeLogEntryPath path: Iterables.concat(
                            Collections.singleton(paths.first()), 
                            paths.second())) {
                        VolumeEntryVoteListener future = VolumeEntryVoteListener.listen(
                                path, 
                                SettableFuturePromise.<Boolean>create(), 
                                cache, service, watch);
                        LoggingFutureListener.listen(logger, future);
                        futures.add(future);
                    }
                    final Votes votes = new Votes(futures.build());
                    final CallablePromiseTask<Votes, ImmutableList<Pair<VolumeLogEntryPath, Boolean>>> task = CallablePromiseTask.create(
                            votes, 
                            SettableFuturePromise.<ImmutableList<Pair<VolumeLogEntryPath, Boolean>>>create());
                    LoggingFutureListener.listen(logger, task);
                    votes.listen(task);
                    Futures.allAsList(votes.votes()).addListener(task, SameThreadExecutor.getInstance());
                    return Optional.of(task);
                } else {
                    return Optional.absent();
                }
            }
        }
        
        protected static final class Votes implements Callable<Optional<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>>> {

            protected final ImmutableList<VolumeEntryVoteListener> votes;
           
            public Votes(ImmutableList<VolumeEntryVoteListener> votes) {
                this.votes = votes;
            }
            
            public ImmutableList<VolumeEntryVoteListener> votes() {
                return votes;
            }
            
            public void listen(ListenableFuture<?> future) {
                new Cancellation(future);
            }

            @Override
            public Optional<ImmutableList<Pair<VolumeLogEntryPath, Boolean>>> call()
                    throws Exception {
                ImmutableList.Builder<Pair<VolumeLogEntryPath, Boolean>> results = ImmutableList.builder();
                for (VolumeEntryVoteListener vote: votes) {
                    if (!vote.isDone()) {
                        return Optional.absent();
                    } else {
                        results.add(Pair.create(vote.path(), vote.get()));
                    }
                }
                return Optional.of(results.build());
            }
            
            protected final class Cancellation implements Runnable {

                protected final ListenableFuture<?> future;
                
                public Cancellation(ListenableFuture<?> future) {
                    this.future = future;
                    future.addListener(this, SameThreadExecutor.getInstance());
                }
                
                @Override
                public void run() {
                    if (future.isCancelled()) {
                        for (VolumeEntryVoteListener vote: votes()) {
                            vote.cancel(false);
                        }
                    }
                }
            }
        }
    }
    
    public static final class Execute extends Action<Boolean> {

        public static Execute create(
                VolumeOperationDirective operation,
                TaskExecutor<VolumeOperationDirective,Boolean> executor) {
            ListenableFuture<Boolean> future;
            try {
                future = executor.submit(operation);
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return new Execute(future);
        }
        
        protected Execute(
                ListenableFuture<Boolean> future) {
            super(future);
        }
    }

    public static final class Commit extends Action<Boolean> {

        public static Commit create(
                final VolumeOperation<?> operation,
                final Collection<VolumeLogEntryPath> entries,
                final Boolean commit,
                final VolumeOperationRequests<?> schema) {
            final Materializer<ControlZNode<?>,?> materializer = schema.schema().getMaterializer();
            final ImmutableList.Builder<Records.MultiOpRequest> multi = ImmutableList.builder();
            for (VolumeLogEntryPath entry: entries) {
                multi.add(schema.schema().version(entry.volume()).entry(entry.entry()).commit().create(commit));
            }
            if (commit.booleanValue()) {
                multi.addAll(schema.apply(operation));
                final UnsignedLong version = operation.getOperator().getParameters().getVersion();
                multi.add(schema.schema().volume(operation.getVolume().getValue()).version(version).latest().update());
                switch (operation.getOperator().getOperator()) {
                case MERGE:
                    multi.add(schema.schema().volume(((MergeParameters) operation.getOperator().getParameters()).getParent().getValue()).version(version).latest().update());
                    break;
                case SPLIT:
                    multi.add(schema.schema().volume(((SplitParameters) operation.getOperator().getParameters()).getLeaf()).version(version).latest().create());
                    break;
                case TRANSFER:
                    break;
                default:
                    throw new AssertionError();
                }
            }
            final ListenableFuture<Boolean> future = Futures.transform(
                    SubmittedRequests.submitRequests(
                            materializer, 
                            new IMultiRequest(multi.build())),
                    new AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean>() {
                        @Override
                        public ListenableFuture<Boolean> apply(
                                List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                            Operations.unlessMultiError((IMultiResponse) Iterables.getOnlyElement(input).record());
                            return Futures.immediateFuture(commit);
                        }
                    },
                    SameThreadExecutor.getInstance());
            return new Commit(future);
        }
        
        protected Commit(
                ListenableFuture<Boolean> future) {
            super(future);
        }
    }
}
