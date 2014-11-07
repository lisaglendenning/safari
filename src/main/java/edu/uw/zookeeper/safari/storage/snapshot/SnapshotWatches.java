package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FourLetterCommand;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ValueFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterWord;
import edu.uw.zookeeper.protocol.FourLetterWords;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode.SessionZNode.SessionIdHex;

public final class SnapshotWatches implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>> {

    public static enum Phase {
        PRE_PHASE, SNAPSHOT, POST_PHASE;
    }
    
    public static <U,V> Transform<U,V> transform(
            Supplier<? extends U> supplier,
            Function<? super U, ? extends V> transformer) {
        return Transform.create(supplier, transformer);
    }

    public static <U,V> FutureTransform<U,V> futureTransform(
            Supplier<? extends ListenableFuture<U>> supplier,
            Function<? super U, ? extends V> transformer) {
        return FutureTransform.create(supplier, transformer);
    }
    
    public static ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> create(
            ZNodePath prefix,
            Function<ZNodePath,ZNodeLabel> labelOf,
            ByteCodec<Object> codec,
            ClientExecutor<? super Records.Request, ?, ?> client,
            Supplier<? extends ListenableFuture<? extends AsyncFunction<Long, Long>>> sessions,
            Iterable<? extends Supplier<? extends ListenableFuture<FourLetterWords.Wchc>>> queries) {
        return create(prefix, labelOf, codec, client, sessions, futureTransform(GetAll.create(queries), Union.create()));
    }
    
    /**
     * Assumes prefix exists.
     */
    public static ChainedFutures.ChainedResult<FourLetterWords.Wchc,?,?,?> create(
            final ZNodePath prefix,
            final Function<ZNodePath,ZNodeLabel> labelOf,
            final ByteCodec<Object> codec,
            final ClientExecutor<? super Records.Request, ?, ?> client,
            final Supplier<? extends ListenableFuture<? extends AsyncFunction<Long, Long>>> sessions,
            final Supplier<? extends ListenableFuture<FourLetterWords.Wchc>> query) {
        final AsyncFunction<Pair<? extends Map<ZNodePath,Long>, Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>>>,FourLetterWords.Wchc> create = new AsyncFunction<Pair<? extends Map<ZNodePath,Long>, Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>>>,FourLetterWords.Wchc>() {
            @Override
            public ListenableFuture<FourLetterWords.Wchc> apply(Pair<? extends Map<ZNodePath,Long>, Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>>> input)
                    throws Exception {
                return CreateWatches.call(
                        prefix, 
                        labelOf, 
                        codec, 
                        client, 
                        input.first(),
                        input.second());
            }
        };
        final AsyncFunction<Pair<FourLetterWords.Wchc, ? extends Map<ZNodePath,Long>>, FourLetterWords.Wchc> translate = new AsyncFunction<Pair<FourLetterWords.Wchc, ? extends Map<ZNodePath,Long>>, FourLetterWords.Wchc>() {
            @Override
            public ListenableFuture<FourLetterWords.Wchc> apply(final Pair<FourLetterWords.Wchc, ? extends Map<ZNodePath,Long>> input) throws Exception {
                return Futures.transform(
                        TranslateSessions.call(input.first(), sessions), 
                        new AsyncFunction<Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>>, FourLetterWords.Wchc>(){
                            @Override
                            public ListenableFuture<FourLetterWords.Wchc> apply(
                                    Queue<ValueFuture<? extends Collection<ZNodePath>, Long, ?>> futures)
                                    throws Exception {
                                return create.apply(Pair.create(input.second(), futures));
                            }
                        });
            }
        };
        return ChainedFutures.<FourLetterWords.Wchc>castLast(
                ChainedFutures.arrayList(
                        new SnapshotWatches(
                                query, 
                                translate), 
                        Phase.values().length));
    }
    
    private final Supplier<? extends ListenableFuture<FourLetterWords.Wchc>> query;
    private final AsyncFunction<Pair<FourLetterWords.Wchc, ? extends Map<ZNodePath,Long>>, FourLetterWords.Wchc> creator;
    
    protected SnapshotWatches(
            Supplier<? extends ListenableFuture<FourLetterWords.Wchc>> query,
            AsyncFunction<Pair<FourLetterWords.Wchc, ? extends Map<ZNodePath,Long>>, FourLetterWords.Wchc> creator) {
        this.query = query;
        this.creator = creator;
    }
    
    @Override
    public Optional<? extends ListenableFuture<?>> apply(
            FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
        if (input.size() >= Phase.values().length) {
            return Optional.absent();
        }
        Phase phase = Phase.values()[input.size()];
        final ListenableFuture<FourLetterWords.Wchc> future;
        switch (phase) {
        case PRE_PHASE:
        {
            future = this.query.get();
            break;
        }
        case SNAPSHOT:
        {
            throw new IllegalStateException();
        }
        case POST_PHASE:
        {
            final ListenableFuture<FourLetterWords.Wchc> query = this.query.get();
            final FourLetterWords.Wchc previous = (FourLetterWords.Wchc) input.get(Phase.PRE_PHASE.ordinal()).get();
            @SuppressWarnings("unchecked")
            final Map<ZNodePath,Long> ephemerals = ((Pair<Long, Map<ZNodePath,Long>>) input.get(Phase.SNAPSHOT.ordinal()).get()).second();
            future = Futures.transform(
                    Futures.transform(
                            query, 
                            new Function<FourLetterWords.Wchc, Pair<FourLetterWords.Wchc, ? extends Map<ZNodePath,Long>>>() {
                                @Override
                                public Pair<FourLetterWords.Wchc, ? extends Map<ZNodePath,Long>> apply(FourLetterWords.Wchc input) {
                                    return Pair.create(Union.create().apply(ImmutableList.of(previous, input)), ephemerals);
                                }
                            }), 
                    creator);
            break;
        }
        default:
            throw new AssertionError();
        }
        return Optional.of(future);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }
    
    public static final class CreateWatches<O extends Operation.ProtocolResponse<?>> extends ToStringListenableFuture<FourLetterWords.Wchc> implements Runnable, Callable<Optional<FourLetterWords.Wchc>>, FutureCallback<CreateWatches<O>.Callback> {
        
        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<FourLetterWords.Wchc> call(
                final ZNodePath prefix,
                final Function<ZNodePath,ZNodeLabel> labelOf,
                final ByteCodec<Object> codec,
                final ClientExecutor<? super Records.Request, O, ?> client,
                final Map<ZNodePath,Long> ephemerals,
                final Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>> values) throws IOException {
            CreateWatches<O> instance = new CreateWatches<O>(values, EntryToRequests.create(prefix, labelOf, Functions.forMap(ephemerals, null), codec), client);
            instance.run();
            return instance;
        }
        
        private final Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, List<Records.Request>> requests;
        private final ClientExecutor<? super Records.Request, O, ?> client;
        private final Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>> values;
        private final ImmutableSetMultimap.Builder<Long, ZNodePath> result;
        private final CallablePromiseTask<CreateWatches<O>, FourLetterWords.Wchc> delegate;
        private final Set<Callback> pending;
        
        protected CreateWatches(
                Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>> values,
                Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, List<Records.Request>> requests,
                ClientExecutor<? super Records.Request, O, ?> client) {
            this.values = values;
            this.requests = requests;
            this.client = client;
            this.result = ImmutableSetMultimap.builder();
            this.pending = Sets.<Callback>newHashSet();
            this.delegate = CallablePromiseTask.create(this, SettableFuturePromise.<FourLetterWords.Wchc>create());
        }
        
        @Override
        public synchronized void run() {
            if (!isDone()) {
                delegate().run();
            } else {
                values.clear();
                pending.clear();
            }
        }
        
        @Override
        public synchronized Optional<FourLetterWords.Wchc> call() throws Exception {
            ValueFuture<? extends Collection<ZNodePath>,Long,?> next;
            while ((next = values.peek()) != null) {
                if (next.isDone()) {
                    values.poll();
                    Long session = next.get();
                    result.putAll(session, next.getValue());
                    List<Records.Request> requests = this.requests.apply(new AbstractMap.SimpleImmutableEntry<Long, Collection<ZNodePath>>(session, next.getValue()));
                    Callback callback = new Callback(SubmittedRequests.submit(client, requests));
                    pending.add(callback);
                    callback.run();
                } else {
                    next.addListener(this, MoreExecutors.directExecutor());
                    break;
                }
            }
            if (values.isEmpty() && pending.isEmpty()) {
                return Optional.of(FourLetterWords.Wchc.fromMultimap(result.build()));
            }
            return Optional.absent();
        }

        @Override
        public synchronized void onSuccess(Callback result) {
            pending.remove(result);
            run();
        }

        @Override
        public void onFailure(Throwable t) {
            delegate().setException(t);
        }

        @Override
        protected CallablePromiseTask<CreateWatches<O>, FourLetterWords.Wchc> delegate() {
            return delegate;
        }
        
        protected final class Callback extends SimpleToStringListenableFuture<List<O>> implements Runnable {

            protected Callback(ListenableFuture<List<O>> delegate) {
                super(delegate);
            }

            @Override
            public void run() {
                if (isDone()) {
                    try {
                        for (O response: get()) {
                            Operations.maybeError(response.record(), KeeperException.Code.NODEEXISTS);
                        }
                    } catch (Exception e) {
                        onFailure(e);
                    }
                    onSuccess(this);
                } else {
                    addListener(this, MoreExecutors.directExecutor());
                }
            }
        }
    }

    protected static final class SessionValuesPaths implements Function<Long, ImmutableList<AbsoluteZNodePath>> {
        
        public static SessionValuesPaths create(
                ZNodePath prefix) {
            return new SessionValuesPaths(prefix);
        }
        
        private final ZNodePath prefix;
        
        public SessionValuesPaths(
                ZNodePath prefix) {
            this.prefix = prefix;
        }
        
        @Override
        public ImmutableList<AbsoluteZNodePath> apply(Long input) {
            final AbsoluteZNodePath session = (AbsoluteZNodePath) prefix.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.labelOf(input.longValue()));
            final AbsoluteZNodePath values = session.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.LABEL);
            return ImmutableList.of(session, values);
        }
    }
    
    /**
     * Not threadsafe.
     */
    public static final class EntryToRequests implements Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, List<Records.Request>> {

        /**
         * Assumes prefix already exists.
         * 
         * @param <O>
         */
        public static EntryToRequests create(
                final ZNodePath prefix,
                final Function<ZNodePath,ZNodeLabel> labelOf,
                final Function<ZNodePath, Long> ephemerals,
                final Serializers.ByteSerializer<Object> serializer) throws IOException {
            return new EntryToRequests(serializer, ephemerals, SessionValuesPaths.create(prefix), labelOf);
        }
        
        private final Serializers.ByteSerializer<Object> serializer;
        private final Operations.Requests.Create createSession;
        private final Operations.Requests.Create createWatch;
        private final Operations.Requests.Create createEphemeral;
        private final Function<ZNodePath, Long> ephemerals;
        private final Function<Long, ImmutableList<AbsoluteZNodePath>> prefix;
        private final Function<ZNodePath,ZNodeLabel> labelOf;
        
        protected EntryToRequests(
                Serializers.ByteSerializer<Object> serializer,
                Function<ZNodePath, Long> ephemerals,
                Function<Long, ImmutableList<AbsoluteZNodePath>> prefix,
                Function<ZNodePath,ZNodeLabel> labelOf) {
            this.serializer = serializer;
            this.createSession = Operations.Requests.create();
            // the only watches reported by wchc/wchp are data watches
            try {
                this.createWatch = Operations.Requests.create().setData(serializer.toBytes(
                        Watcher.WatcherType.Data));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            this.createEphemeral = Operations.Requests.create();
            this.ephemerals = ephemerals;
            this.prefix = prefix;
            this.labelOf = labelOf;
        }
        
        @Override
        public List<Records.Request> apply(Map.Entry<Long, ? extends Collection<ZNodePath>> input) {
            List<Records.Request> requests = Lists.newArrayListWithCapacity(input.getValue().size()*2+2);
            for (ZNodePath path: prefix.apply(input.getKey())) {
                requests.add(createSession.setPath(path).build());
            }
            for (ZNodePath watch: input.getValue()) {
                requests.add(createWatch.setPath(createSession.getPath().join(labelOf.apply(watch))).build());
                Long ephemeral = this.ephemerals.apply(watch);
                if (ephemeral != null) {
                    try {
                        requests.add(createEphemeral.setPath(createWatch.getPath().join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch.Ephemeral.LABEL))
                                .setData(serializer.toBytes(SessionIdHex.valueOf(ephemeral.longValue()))).build());
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            }
            return requests;
        }
    }

    public static final class TranslateSessions implements Function<AsyncFunction<Long, Long>, Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>>> {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>>> call(
                FourLetterWords.Wchc wchc,
                Supplier<? extends ListenableFuture<? extends AsyncFunction<Long, Long>>> sessions) {
            return Futures.transform(sessions.get(), new TranslateSessions(wchc));
        }

        private final FourLetterWords.Wchc wchc;
        
        protected TranslateSessions(
                FourLetterWords.Wchc wchc) {
            this.wchc = wchc;
        }

        @Override
        public Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>> apply(
                AsyncFunction<Long, Long> input) {
            Queue<ValueFuture<? extends Collection<ZNodePath>,Long,?>> lookups= new ArrayDeque<ValueFuture<? extends Collection<ZNodePath>,Long,?>>(wchc.keySet().size());
            for (Map.Entry<Long, Collection<ZNodePath>> entry: wchc) {
                ListenableFuture<Long> future;
                try {
                    future = input.apply(entry.getKey());
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                lookups.add(ValueFuture.create(entry.getValue(), future));
            }
            return lookups;
        }
    }

    public static final class StringToWchc implements Function<String, FourLetterWords.Wchc> {

        public static StringToWchc create() {
            return new StringToWchc();
        }
        
        protected StringToWchc() {}
        
        @Override
        public FourLetterWords.Wchc apply(String input) {
            return FourLetterWords.Wchc.fromString(input);
        }
    }

    public static final class FilteredWchc implements Function<FourLetterWords.Wchc, FourLetterWords.Wchc> {

        public static FilteredWchc create(
                Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, ? extends Collection<ZNodePath>> filter) {
            return new FilteredWchc(filter);
        }
        
        private final Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, ? extends Collection<ZNodePath>> filter;
        
        public FilteredWchc(
                Function<Map.Entry<Long, ? extends Collection<ZNodePath>>, ? extends Collection<ZNodePath>> filter) {
            this.filter = filter;
        }
        
        @Override
        public FourLetterWords.Wchc apply(FourLetterWords.Wchc input) {
            ImmutableSetMultimap.Builder<Long, ZNodePath> builder = ImmutableSetMultimap.builder();
            for (Map.Entry<Long, ? extends Collection<ZNodePath>> entry: input) {
                Collection<ZNodePath> filtered = filter.apply(entry);
                if (!filtered.isEmpty()) {
                    builder.putAll(entry.getKey(), filtered);
                }
            }
            return FourLetterWords.Wchc.fromMultimap(builder.build());
        }
    }
    
    public static final class Union implements Function<Iterable<FourLetterWords.Wchc>, FourLetterWords.Wchc> {

        public static Union create() {
            return new Union();
        }
        
        protected Union() {}
        
        @Override
        public FourLetterWords.Wchc apply(Iterable<FourLetterWords.Wchc> input) {
            ImmutableSetMultimap.Builder<Long, ZNodePath> builder = ImmutableSetMultimap.builder();
            for (FourLetterWords.Wchc wchc: input) {
                builder.putAll(wchc.asMultimap());
            }
            return FourLetterWords.Wchc.fromMultimap(builder.build());
        }
    }
    
    public static final class GetAll<V> implements Supplier<ListenableFuture<List<V>>> {

        public static <V> GetAll<V> create(
                Iterable<? extends Supplier<? extends ListenableFuture<? extends V>>> suppliers) {
            return new GetAll<V>(ImmutableList.copyOf(suppliers));
        }
        
        private final ImmutableList<? extends Supplier<? extends ListenableFuture<? extends V>>> suppliers;
        
        protected GetAll(ImmutableList<? extends Supplier<? extends ListenableFuture<? extends V>>> suppliers) {
            this.suppliers = suppliers;
        }

        @Override
        public ListenableFuture<List<V>> get() {
            ImmutableList.Builder<ListenableFuture<? extends V>> futures = ImmutableList.builder();
            for (Supplier<? extends ListenableFuture<? extends V>> supplier: suppliers) {
                futures.add(supplier.get());
            }
            return Futures.allAsList(futures.build());
        }
    }

    public static final class FutureTransform<U,V> implements Supplier<ListenableFuture<V>> {

        public static <U,V> FutureTransform<U,V> create(
                Supplier<? extends ListenableFuture<U>> supplier,
                Function<? super U, ? extends V> transformer) {
            return new FutureTransform<U,V>(supplier, transformer);
        }
        
        private final Supplier<? extends ListenableFuture<U>> supplier;
        private final Function<? super U, ? extends V> transformer;
        
        protected FutureTransform(
                Supplier<? extends ListenableFuture<U>> supplier,
                Function<? super U, ? extends V> transformer) {
            this.supplier = supplier;
            this.transformer = transformer;
        }

        @Override
        public ListenableFuture<V> get() {
            return Futures.transform(supplier.get(), transformer);
        }
    }

    public static final class Transform<U,V> implements Supplier<V> {

        public static <U,V> Transform<U,V> create(
                Supplier<? extends U> supplier,
                Function<? super U, ? extends V> transformer) {
            return new Transform<U,V>(supplier, transformer);
        }
        
        private final Supplier<? extends U> supplier;
        private final Function<? super U, ? extends V> transformer;
        
        protected Transform(
                Supplier<? extends U> supplier,
                Function<? super U, ? extends V> transformer) {
            this.supplier = supplier;
            this.transformer = transformer;
        }

        @Override
        public V get() {
            return transformer.apply(supplier.get());
        }
    }
    
    public static final class QueryWchc implements Function<ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>, ListenableFuture<FourLetterWords.Wchc>> {

        public static QueryWchc create() {
            return new QueryWchc(StringToWchc.create());
        }
        
        private final Function<String, FourLetterWords.Wchc> transformer;
        
        protected QueryWchc(
                        Function<String, FourLetterWords.Wchc> transformer) {
            this.transformer = transformer;
        }
    
        @Override
        public ListenableFuture<FourLetterWords.Wchc> apply(ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> input) {
            return Futures.transform(
                    FourLetterCommand.callThenClose(
                        FourLetterWord.WCHC, 
                        input),
                    transformer);
        }
    }
}
