package edu.uw.zookeeper.safari.control.volumes;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.FutureChain.FutureDequeChain;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.volumes.VolumeEntryResponse;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationDirective;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationProposer;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

public final class VolumeOperationCoordinator extends ToStringListenableFuture.SimpleToStringListenableFuture<Boolean> {

    public static VolumeOperationCoordinator forEntry(
            final VolumeOperationCoordinatorEntry entry,
            final TaskExecutor<VolumeOperationDirective,Boolean> executor,
            final AsyncFunction<? super VersionedId, VolumeVersion<?>> states,
            final Service service,
            final SchemaClientService<ControlZNode<?>,?> control) {
        ListenableFuture<Pair<VolumeLogEntryPath,Optional<VolumeLogEntryPath>>> entries = LoggingFutureListener.listen(
                LogManager.getLogger(VolumeOperationCoordinator.class), 
                VolumeOperationProposer.forProposal(
                    entry, control.materializer()));
        VolumeOperationCoordinator instance = 
                LoggingFutureListener.listen(
                        LogManager.getLogger(VolumeOperationCoordinator.class), 
                        create(
                            entry.operation(), 
                            entries, 
                            executor,
                            states,
                            service,
                            control));
        return instance;
    }
    
    public static VolumeOperationCoordinator create(
            final VolumeOperation<?> operation,
            final ListenableFuture<Pair<VolumeLogEntryPath,Optional<VolumeLogEntryPath>>> entries,
            final TaskExecutor<VolumeOperationDirective,Boolean> executor,
            final AsyncFunction<? super VersionedId, VolumeVersion<?>> states,
            final Service service,
            final SchemaClientService<ControlZNode<?>,?> control) {
        final Supplier<Propose> proposer = new Supplier<Propose>() {
            @Override
            public Propose get() {
                return Propose.create(
                        entries, 
                        control, 
                        service);
            }
        };
        final Apply apply = new Apply(
                operation, 
                proposer, 
                executor, 
                VolumeOperationRequests.create(
                        VolumesSchemaRequests.create(
                                control.materializer()), states),
                LogManager.getLogger(VolumeOperationCoordinator.class));
        ChainedFutures.ChainedResult<Boolean, ?, ?, ?> result = 
                ChainedFutures.result(
                        new Processor<FutureChain.FutureDequeChain<? extends ListenableFuture<?>>, Boolean>() {
                            @Override
                            public Boolean apply(
                                    FutureDequeChain<? extends ListenableFuture<?>> input)
                                    throws Exception {
                                Iterator<? extends ListenableFuture<?>> previous = input.descendingIterator();
                                while (previous.hasNext()) {
                                    previous.next().get();
                                }
                                return (Boolean) input.getLast().get();
                            }
                        },
                    ChainedFutures.arrayDeque(
                            apply));
        return new VolumeOperationCoordinator(apply, ChainedFutures.run(result));
    }

    protected final Apply call;
    
    protected VolumeOperationCoordinator(
            Apply call, ListenableFuture<Boolean> delegate) {
        super(delegate);
        this.call = call;
    }
    
    public VolumeOperation<?> operation() {
        return call.operation();
    }
    
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        return super.toStringHelper(helper.addValue(operation()));
    }

    protected static final class Apply implements ChainedFutures.ChainedProcessor<VolumeOperationCoordinator.Action<?>, ChainedFutures.DequeChain<Action<?>, ?>> {

        protected final Logger logger;
        protected final VolumeOperation<?> operation;
        protected final Supplier<Propose> proposer;
        protected final TaskExecutor<VolumeOperationDirective,Boolean> executor;
        protected final VolumeOperationRequests<?> requests;
        
        public Apply(
                VolumeOperation<?> operation,
                Supplier<Propose> proposer,
                TaskExecutor<VolumeOperationDirective,Boolean> executor,
                VolumeOperationRequests<?> requests,
                Logger logger) {
            this.logger = logger;
            this.operation = operation;
            this.proposer = proposer;
            this.executor = executor;
            this.requests = requests;
        }
        
        public VolumeOperation<?> operation() {
            return operation;
        }
        
        @Override
        public Optional<? extends Action<?>> apply(ChainedFutures.DequeChain<Action<?>,?> input) throws Exception {
            if (input.isEmpty()) {
                logger.info("PROPOSING {}", operation);
                return Optional.of(proposer.get());
            }
            Iterator<Action<?>> previous = input.descendingIterator();
            Action<?> last = previous.next();
            if (last instanceof Propose) {
                VolumeLogEntryPath first = null;
                Boolean commit = Boolean.TRUE;
                for (Pair<VolumeLogEntryPath,Boolean> e: ((Propose) last).get()) {
                    if (first == null) {
                        first = e.first();
                    }
                    if (e.second().equals(Boolean.FALSE)) {
                        commit = Boolean.FALSE;
                        break;
                    }
                }
                assert (first != null);
                VolumeOperationDirective directive = VolumeOperationDirective.create(first, operation, commit);
                logger.info("EXECUTING {}", directive);
                return Optional.of(Execute.create(
                        directive,
                        executor));
            } else if (last instanceof Execute) {
                Boolean commit;
                try {
                    commit = ((Execute) last).get();
                } catch (Exception e) {
                    commit = Boolean.FALSE;
                }
                Propose propose = (Propose) previous.next();
                Iterable<Pair<VolumeLogEntryPath, Boolean>> votes = propose.get();
                ImmutableList.Builder<VolumeLogEntryPath> entries = ImmutableList.builder();
                for (Pair<VolumeLogEntryPath, Boolean> vote: votes) {
                    entries.add(vote.first());
                }
                if (logger.isInfoEnabled()) {
                    if (commit.booleanValue()) {
                        logger.info("COMMITING {}", operation);
                    } else {
                        logger.info("ABORTING {}", operation);
                    }
                }
                try {
                    return Optional.of(Commit.create(operation, entries.build(), commit, requests));
                } catch (Exception e) {
                    return Optional.absent();
                }
            } else {
                return Optional.absent();
            }
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(operation).toString();
        }
    }
    
    public static abstract class Action<V> extends SimpleToStringListenableFuture<V> {
        
        protected Action(ListenableFuture<V> future) {
            super(future);
        }
    }
    
    public static final class Propose extends Action<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> {

        public static Propose create(
                ListenableFuture<Pair<VolumeLogEntryPath,Optional<VolumeLogEntryPath>>> entries,
                SchemaClientService<ControlZNode<?>,?> client,
                Service service) {
            return new Propose(
                    Callback.create(
                            entries, 
                            client, 
                            service,
                            LogManager.getLogger(Propose.class)));
        }
        
        protected Propose(
                ListenableFuture<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> future) {
            super(future);
        }

        protected static final class Callback implements AsyncFunction<Pair<VolumeLogEntryPath,Optional<VolumeLogEntryPath>>, ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> {

            public static ListenableFuture<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> create(
                    ListenableFuture<Pair<VolumeLogEntryPath,Optional<VolumeLogEntryPath>>> entries,
                    SchemaClientService<ControlZNode<?>,?> client,
                    Service service,
                    Logger logger) {
                return Futures.transform(
                        entries,
                        new Callback(client, service, logger));
            }
            
            protected final SchemaClientService<ControlZNode<?>,?> client;
            protected final Service service;
            protected final Logger logger;
            
            protected Callback(
                    SchemaClientService<ControlZNode<?>,?> client,
                    Service service,
                    Logger logger) {
                this.logger = logger;
                this.client = client;
                this.service = service;
            }
            
            @Override
            public ListenableFuture<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> apply(
                    Pair<VolumeLogEntryPath,Optional<VolumeLogEntryPath>> input) throws Exception {
                final ImmutableList.Builder<VolumeEntryResponse> futures = ImmutableList.builder();
                for (VolumeLogEntryPath path: input.second().isPresent() ? 
                        ImmutableList.of(input.first(), input.second().get()) : 
                            ImmutableList.of(input.first())) {
                    VolumeEntryResponse future = 
                            LoggingFutureListener.listen(
                                    logger,
                                    VolumeEntryResponse.voted(
                                        path, 
                                        client.materializer(),
                                        client.cacheEvents(),
                                        logger));
                    futures.add(future);
                }
                return LoggingFutureListener.listen(logger, Votes.create(futures.build()));
            }
        }
        
        protected static final class Votes implements Callable<Optional<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>>> {

            public static ListenableFuture<ImmutableList<Pair<VolumeLogEntryPath,Boolean>>> create(
                    ImmutableList<VolumeEntryResponse> votes) {
                final Votes instance = new Votes(votes);
                final CallablePromiseTask<Votes, ImmutableList<Pair<VolumeLogEntryPath, Boolean>>> task = CallablePromiseTask.create(
                        instance, 
                        SettableFuturePromise.<ImmutableList<Pair<VolumeLogEntryPath, Boolean>>>create());
                instance.listen(task);
                Futures.allAsList(votes).addListener(task, MoreExecutors.directExecutor());
                return task;
            }
            
            protected final ImmutableList<VolumeEntryResponse> votes;
           
            public Votes(ImmutableList<VolumeEntryResponse> votes) {
                this.votes = votes;
            }
            
            public ImmutableList<VolumeEntryResponse> votes() {
                return votes;
            }
            
            public void listen(ListenableFuture<?> future) {
                new Cancellation(future);
            }

            @Override
            public Optional<ImmutableList<Pair<VolumeLogEntryPath, Boolean>>> call()
                    throws Exception {
                ImmutableList.Builder<Pair<VolumeLogEntryPath, Boolean>> results = ImmutableList.builder();
                for (VolumeEntryResponse vote: votes) {
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
                    future.addListener(this, MoreExecutors.directExecutor());
                }
                
                @Override
                public void run() {
                    if (future.isDone()) {
                        if (future.isCancelled()) {
                            for (VolumeEntryResponse vote: votes()) {
                                vote.cancel(false);
                            }
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
            ListenableFuture<List<Records.MultiOpRequest>> requests;
            try {
                requests = schema.apply(operation);
            } catch (Exception e) {
                requests = Futures.immediateFailedFuture(e);
            }
            return new Commit(Futures.transform(
                    requests, 
                    new Submit(operation, entries, commit, schema.schema())));
        }
        
        protected Commit(
                ListenableFuture<Boolean> future) {
            super(future);
        }
        
        protected final static class Submit implements AsyncFunction<List<Records.MultiOpRequest>, Boolean> {

            private final VolumeOperation<?> operation;
            private final Collection<VolumeLogEntryPath> entries;
            private final Boolean commit;
            private final VolumesSchemaRequests<?> schema;
            
            protected Submit(
                    final VolumeOperation<?> operation,
                    final Collection<VolumeLogEntryPath> entries,
                    final Boolean commit,
                    final VolumesSchemaRequests<?> schema) {
                this.operation = operation;
                this.entries = entries;
                this.commit = commit;
                this.schema = schema;
            }
            
            @Override
            public ListenableFuture<Boolean> apply(List<Records.MultiOpRequest> input)
                    throws Exception {
                final ImmutableList.Builder<Records.MultiOpRequest> multi = ImmutableList.builder();
                for (VolumeLogEntryPath entry: entries) {
                    multi.add(schema.version(entry.volume()).entry(entry.entry()).commit().create(commit));
                }
                if (commit.booleanValue()) {
                    multi.addAll(input);
                    final UnsignedLong version = operation.getOperator().getParameters().getVersion();
                    multi.add(schema.volume(operation.getVolume().getValue()).version(version).latest().update());
                    switch (operation.getOperator().getOperator()) {
                    case MERGE:
                        multi.add(schema.volume(((MergeParameters) operation.getOperator().getParameters()).getParent().getValue()).version(version).latest().update());
                        break;
                    case SPLIT:
                        multi.add(schema.volume(((SplitParameters) operation.getOperator().getParameters()).getLeaf()).version(version).latest().create());
                        break;
                    case TRANSFER:
                        break;
                    default:
                        throw new AssertionError();
                    }
                }
                return Futures.transform(
                        SubmittedRequests.submitRequests(
                                schema.getMaterializer(), 
                                new IMultiRequest(multi.build())),
                        new AsyncFunction<List<? extends Operation.ProtocolResponse<?>>, Boolean>() {
                            @Override
                            public ListenableFuture<Boolean> apply(
                                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                                Operations.unlessMultiError((IMultiResponse) Iterables.getOnlyElement(input).record());
                                return Futures.immediateFuture(commit);
                            }
                        });
            }
        }
    }
}
