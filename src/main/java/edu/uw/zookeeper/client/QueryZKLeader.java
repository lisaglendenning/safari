package edu.uw.zookeeper.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterWord;
import edu.uw.zookeeper.protocol.FourLetterWords;
import edu.uw.zookeeper.protocol.Message;

public class QueryZKLeader extends ForwardingListenableFuture<List<Boolean>> implements Callable<Optional<Optional<ServerInetAddressView>>> {

    public static ListenableFuture<Optional<ServerInetAddressView>> call(
            EnsembleView<ServerInetAddressView> ensemble,
            ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> connections) {
        ImmutableMap.Builder<ListenableFuture<Boolean>, ServerInetAddressView> futures = ImmutableMap.builder();
        for (ServerInetAddressView server: ensemble) {
            futures.put(
                    Futures.transform(
                        FourLetterCommand.callThenClose(
                                FourLetterWord.MNTR, 
                                connections.connect(server.get())),
                        Functions.compose(
                                new MntrServerStateIsLeader(), 
                                new GetMntrServerState())), 
                    server);
        }
        ImmutableMap<ListenableFuture<Boolean>, ServerInetAddressView> queries = futures.build();
        CallablePromiseTask<QueryZKLeader, Optional<ServerInetAddressView>> task = CallablePromiseTask.create(
                new QueryZKLeader(queries), 
                SettableFuturePromise.<Optional<ServerInetAddressView>>create());
        for (ListenableFuture<Boolean> future: queries.keySet()) {
            future.addListener(task, MoreExecutors.directExecutor());
        }
        return task;
    }
    
    protected final ListenableFuture<List<Boolean>> all;
    protected final ImmutableMap<ListenableFuture<Boolean>, ServerInetAddressView> queries;
    
    protected QueryZKLeader(ImmutableMap<ListenableFuture<Boolean>, ServerInetAddressView> queries) {
        this.queries = queries;
        this.all = Futures.successfulAsList(queries.keySet());
    }
    
    @Override
    public Optional<Optional<ServerInetAddressView>> call() throws Exception {
        for (Map.Entry<ListenableFuture<Boolean>, ServerInetAddressView> query: queries.entrySet()) {
            if (query.getKey().isDone()) {
                try {
                    if (query.getKey().get().booleanValue()) {
                        cancel(false);
                        return Optional.of(Optional.of(query.getValue()));
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                }
            }
        }
        if (isDone()) {
            return Optional.of(Optional.<ServerInetAddressView>absent());
        } else {
            return Optional.absent();
        }
    }

    @Override
    protected ListenableFuture<List<Boolean>> delegate() {
        return all;
    }

    public static class MntrServerStateIsLeader implements Function<FourLetterWords.Mntr.MntrServerState, Boolean> {

        public MntrServerStateIsLeader() {}
        
        @Override
        public Boolean apply(FourLetterWords.Mntr.MntrServerState input) {
            switch (input) {
            case LEADER:
            case STANDALONE:
                return Boolean.TRUE;
            default:
                return Boolean.FALSE;
            }
        }
    }
    
    public static class GetMntrServerState implements Function<String, FourLetterWords.Mntr.MntrServerState> {

        public GetMntrServerState() {}
        
        @Override
        public FourLetterWords.Mntr.MntrServerState apply(String input) {
            FourLetterWords.Mntr mntr;
            try {
                mntr = FourLetterWords.Mntr.fromString(input);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return mntr.getValue(FourLetterWords.Mntr.getMntrValueType(
                    FourLetterWords.Mntr.MntrKey.ZK_SERVER_STATE, 
                    FourLetterWords.Mntr.MntrServerState.class));
        }
    }
}
