package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class VolumeEntryAcceptor implements Callable<ListenableFuture<Optional<? extends Sequential<String,?>>>> {

    public static VolumeEntryAcceptor defaults(
            final VersionedId volume,
            final Service service,
            final ControlClientService control) {
        final VolumeEntryVoter<?> voter = VolumeEntryVoter.selectFirst(volume, control.materializer());
        final Callable<ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed = 
                committed(service, 
                          control.cacheEvents(),
                          volume,
                          control.materializer().cache());
        return new VolumeEntryAcceptor(volume, Apply.create(voter, committed));
    }
    
    public static Callable<ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed(
            final Service service,
            final WatchListeners watch,
            final VersionedId volume,
            final LockableZNodeCache<ControlZNode<?>, Records.Request, ?> cache) {
        final VolumeEntryCommitListener.VotedEntriesCommitted committed = VolumeEntryCommitListener.committed(volume, cache);
        return new Callable<ListenableFuture<? extends Optional<? extends Sequential<String,?>>>>() {
            @Override
            public ListenableFuture<? extends Optional<? extends Sequential<String,?>>> call() {
                return VolumeEntryCommitListener.listen(
                    service,
                    watch,
                    committed);
            }
        };
    }

    private final VersionedId volume;
    private final Function<List<VolumeEntryAcceptor.Action<?>>, Optional<? extends VolumeEntryAcceptor.Action<?>>> apply;
    
    protected VolumeEntryAcceptor(
            VersionedId volume,
            Function<List<VolumeEntryAcceptor.Action<?>>, Optional<? extends VolumeEntryAcceptor.Action<?>>> apply) {
        this.volume = volume;
        this.apply = apply;
    }
    
    public VersionedId volume() {
        return volume;
    }
    
    @Override
    public ListenableFuture<Optional<? extends Sequential<String, ?>>> call()
            throws Exception {
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(
                                apply, 
                                Lists.<Action<?>>newLinkedList()),
                        ChainedFutures.<Optional<? extends Sequential<String,?>>>castLast()), 
                SettableFuturePromise.<Optional<? extends Sequential<String,?>>>create());
    }
    
    protected static class Apply implements Function<List<VolumeEntryAcceptor.Action<?>>, Optional<? extends VolumeEntryAcceptor.Action<?>>> {

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
        public Optional<? extends Action<?>> apply(List<Action<?>> input) {
            // check the last action
            Optional<? extends Action<?>> last = input.isEmpty() ? Optional.<Action<?>>absent() : Optional.of(input.get(input.size()-1));
            if (last.isPresent()) {
                try {
                    last.get().get();
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                } catch (Exception e) {
                    return Optional.absent();
                }
                if (last.get() instanceof Commit) {
                    return Optional.absent();
                }
            }
            
            Optional<? extends ListenableFuture<?>> vote;
            try {
                vote = voter.call();
            } catch (Exception e) {
                vote = Optional.of(Futures.immediateFailedFuture(e));
            }
            
            if (vote.isPresent()) {
                return Optional.of(Vote.create(vote.get()));
            } else {
                ListenableFuture<? extends Optional<? extends Sequential<String,?>>> commit;
                try {
                    commit = committed.call();
                } catch (Exception e) {
                    commit = Futures.immediateFailedFuture(e);
                }
                return Optional.of(Commit.create(commit));
            }
        }
    }

    protected static abstract class Action<V> extends SimpleToStringListenableFuture<V> {
        
        protected Action(ListenableFuture<V> future) {
            super(future);
        }
    }
    
    protected static final class Vote<V> extends Action<V> {
        
        public static <V> Vote<V> create(ListenableFuture<V> future) {
            return new Vote<V>(future);
        }
        
        protected Vote(ListenableFuture<V> future) {
            super(future);
        }
    }
    
    protected static final class Commit<V extends Optional<? extends Sequential<String,?>>> extends Action<V> {

        public static <V extends Optional<? extends Sequential<String,?>>> Commit<V> create(ListenableFuture<V> future) {
            return new Commit<V>(future);
        }
        
        protected Commit(ListenableFuture<V> future) {
            super(future);
        }
    }
}
