package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ChainedFutures.ChainedProcessor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class VolumeEntryAcceptor implements Supplier<ListenableFuture<Optional<? extends Sequential<String,?>>>> {

    public static VolumeEntryAcceptor defaults(
            final VersionedId version,
            final Materializer<ControlZNode<?>,?> materializer,
            final WatchListeners cacheEvents) {
        final VolumeEntryVoter<?> voter = VolumeEntryVoter.selectFirst(
                version, 
                materializer);
        final Callable<VolumeEntriesCommitted> committed = 
                committed(
                        version,
                        materializer.cache(), 
                        cacheEvents);
        return new VolumeEntryAcceptor(version, Apply.create(voter, committed));
    }
    
    public static Callable<VolumeEntriesCommitted> committed(
            final VersionedId version,
            final LockableZNodeCache<ControlZNode<?>,?,?> cache,
            final WatchListeners cacheEvents) {
        return new Callable<VolumeEntriesCommitted>() {
            @Override
            public VolumeEntriesCommitted call() {
                return VolumeEntriesCommitted.create(
                    version,
                    cache,
                    cacheEvents);
            }
        };
    }

    private final VersionedId version;
    private final ChainedProcessor<VolumeEntryAcceptor.Action<?>> apply;
    
    protected VolumeEntryAcceptor(
            VersionedId version,
            ChainedProcessor<VolumeEntryAcceptor.Action<?>> apply) {
        this.version = version;
        this.apply = apply;
    }
    
    public VersionedId version() {
        return version;
    }
    
    @Override
    public ListenableFuture<Optional<? extends Sequential<String, ?>>> get() {
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(
                                apply, 
                                Lists.<Action<?>>newLinkedList()),
                        ChainedFutures.<Optional<? extends Sequential<String,?>>>castLast()), 
                SettableFuturePromise.<Optional<? extends Sequential<String,?>>>create());
    }
    
    protected static final class Apply implements ChainedProcessor<VolumeEntryAcceptor.Action<?>> {

        public static Apply create(
                Callable<? extends Optional<? extends ListenableFuture<?>>> voter,
                Callable<? extends ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed) {
            return new Apply(voter, committed);
        }
        
        protected final Callable<? extends Optional<? extends ListenableFuture<?>>> voter;
        protected final Callable<? extends ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed;
        
        protected Apply(
                Callable<? extends Optional<? extends ListenableFuture<?>>> voter,
                Callable<? extends ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed) {
            this.voter = voter;
            this.committed = committed;
        }
        
        @Override
        public Optional<? extends Action<?>> apply(List<Action<?>> input) throws Exception {
            // check the last action
            Optional<? extends Action<?>> last = input.isEmpty() ? Optional.<Action<?>>absent() : Optional.of(input.get(input.size()-1));
            if (last.isPresent()) {
                if (last.get() instanceof Committed) {
                    return Optional.absent();
                }
                try {
                    last.get().get();
                } catch (ExecutionException e) {
                    return Optional.absent();
                }
            }
            Optional<? extends Vote<?>> vote = Vote.call(voter);
            if (vote.isPresent()) {
                return vote;
            } else {
                return Optional.of(Committed.call(committed));
            }
        }
    }

    protected static abstract class Action<V> extends SimpleToStringListenableFuture<V> {
        
        protected Action(ListenableFuture<V> future) {
            super(future);
        }
    }
    
    protected static final class Vote<V> extends Action<V> {

        public static Optional<? extends Vote<?>> call(Callable<? extends Optional<? extends ListenableFuture<?>>> voter) {
            Optional<? extends ListenableFuture<?>> vote;
            try {
                vote = voter.call();
            } catch (Exception e) {
                vote = Optional.of(Futures.immediateFailedFuture(e));
            }
            if (vote.isPresent()) {
                return Optional.of(create(vote.get()));
            } else {
                return Optional.absent();
            }
        }
        
        public static <V> Vote<V> create(ListenableFuture<V> future) {
            return new Vote<V>(future);
        }
        
        protected Vote(ListenableFuture<V> future) {
            super(future);
        }
    }
    
    protected static final class Committed<V extends Optional<? extends Sequential<String,?>>> extends Action<V> {

        public static Committed<?> call(Callable<? extends ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed) {
            ListenableFuture<? extends Optional<? extends Sequential<String,?>>> future;
            try {
                future = committed.call();
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return create(future);
        }
        
        public static <V extends Optional<? extends Sequential<String,?>>> Committed<V> create(ListenableFuture<V> future) {
            return new Committed<V>(future);
        }
        
        protected Committed(ListenableFuture<V> future) {
            super(future);
        }
    }
}
