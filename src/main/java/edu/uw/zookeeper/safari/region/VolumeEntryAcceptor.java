package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.ControlClientService;

public class VolumeEntryAcceptor implements Function<List<VolumeEntryAcceptor.Action<?>>, Optional<? extends VolumeEntryAcceptor.Action<?>>> {

    public static ListenableFuture<Optional<? extends Sequential<String,?>>> defaults(
            final VersionedId volume,
            final Service service,
            final ControlClientService control) {
        VolumeEntryVoter voter = VolumeEntryVoter.selectFirst(volume, control.materializer());
        Callable<ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed = 
                new Callable<ListenableFuture<? extends Optional<? extends Sequential<String,?>>>>() {
                    @Override
                    public ListenableFuture<? extends Optional<? extends Sequential<String,?>>> call() {
                        return VolumeEntryCommitListener.listen(
                            service,
                            control.cacheEvents(),
                            volume,
                            control.materializer().cache(),
                            SettableFuturePromise.<Optional<Sequential<String,?>>>create());
                    }
                };
        return create(voter, committed);
    }

    public static ListenableFuture<Optional<? extends Sequential<String,?>>> create(
            Callable<? extends Optional<? extends ListenableFuture<?>>> voter,
            Callable<? extends ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed) {
        return ChainedFutures.run(
                ChainedFutures.process(
                        ChainedFutures.chain(
                                new VolumeEntryAcceptor(voter, committed), 
                                Lists.<Action<?>>newLinkedList()),
                        ChainedFutures.<Optional<? extends Sequential<String,?>>>castLast()), 
                SettableFuturePromise.<Optional<? extends Sequential<String,?>>>create());
    }

    protected final Callable<? extends Optional<? extends ListenableFuture<?>>> voter;
    protected final Callable<? extends ListenableFuture<? extends Optional<? extends Sequential<String,?>>>> committed;
    
    protected VolumeEntryAcceptor(
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
    
    public static abstract class Action<V> extends ForwardingListenableFuture<V> {
        
        protected final ListenableFuture<V> future;

        protected Action(ListenableFuture<V> future) {
            this.future = future;
        }
        
        @Override
        protected ListenableFuture<V> delegate() {
            return future;
        }
    }
    
    public static class Vote<V> extends Action<V> {
        
        public static <V> Vote<V> create(ListenableFuture<V> future) {
            return new Vote<V>(future);
        }
        
        protected Vote(ListenableFuture<V> future) {
            super(future);
        }
    }
    
    public static class Commit<V extends Optional<? extends Sequential<String,?>>> extends Action<V> {

        public static <V extends Optional<? extends Sequential<String,?>>> Commit<V> create(ListenableFuture<V> future) {
            return new Commit<V>(future);
        }
        
        protected Commit(ListenableFuture<V> future) {
            super(future);
        }
    }
}
