package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.WatchType;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.FourLetterCommand;
import edu.uw.zookeeper.client.SubmittedRequest;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers.ByteCodec;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterWord;
import edu.uw.zookeeper.protocol.FourLetterWords;
import edu.uw.zookeeper.protocol.FourLetterWords.Wchc;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.snapshot.ExecuteSnapshot.ValueSessionLookup;

public class SnapshotEnsembleWatches implements Function<List<ListenableFuture<FourLetterWords.Wchc>>, Optional<? extends ListenableFuture<FourLetterWords.Wchc>>> {

    public static SnapshotEnsembleWatches forServers(
            ZNodePath prefix,
            Function<ZNodePath,ZNodeLabel> labelOf,
            ByteCodec<Object> codec,
            ClientExecutor<? super Records.Request, ?, ?> client,
            ImmutableList<QueryServerWatches> servers) {
        return create(prefix, labelOf, codec, client, QueryEnsembleWatches.forServers(servers));
    }
    
    public static SnapshotEnsembleWatches create(
            ZNodePath prefix,
            Function<ZNodePath,ZNodeLabel> labelOf,
            ByteCodec<Object> codec,
            ClientExecutor<? super Records.Request, ?, ?> client,
            Supplier<ListenableFuture<PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>>> query) {
        return new SnapshotEnsembleWatches(prefix, labelOf, codec, client, query);
    }
    
    private final Supplier<ListenableFuture<PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>>> query;
    private final ZNodePath prefix;
    private final Function<ZNodePath,ZNodeLabel> labelOf;
    private final ByteCodec<Object> codec;
    private final ClientExecutor<? super Records.Request, ?, ?> client;
    
    protected SnapshotEnsembleWatches(
            ZNodePath prefix,
            Function<ZNodePath,ZNodeLabel> labelOf,
            ByteCodec<Object> codec,
            ClientExecutor<? super Records.Request, ?, ?> client,
            Supplier<ListenableFuture<PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>>> query) {
        this.prefix = prefix;
        this.labelOf = labelOf;
        this.codec = codec;
        this.client = client;
        this.query = query;
    }
    
    @Override
    public Optional<? extends ListenableFuture<FourLetterWords.Wchc>> apply(
            List<ListenableFuture<FourLetterWords.Wchc>> input) {
        if (input.size() == 2) {
            return Optional.absent();
        }
        final ImmutableSetMultimap.Builder<Long, ZNodePath> builder;
        if (input.isEmpty()) {
            builder = ImmutableSetMultimap.builder();
        } else {
            FourLetterWords.Wchc wchc;
            try {
                wchc = (FourLetterWords.Wchc) input.get(input.size()-1).get();
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            } catch (ExecutionException e) {
                return Optional.absent();
            }
            builder = ImmutableSetMultimap.<Long, ZNodePath>builder().putAll(wchc.asMultimap());
        }
        final ListenableFuture<FourLetterWords.Wchc> wchc =
            Futures.transform(
                    query.get(), 
                    new AsyncFunction<PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>,FourLetterWords.Wchc>() {
                        @Override
                        public ListenableFuture<FourLetterWords.Wchc> apply(PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>> input) throws Exception {
                            final IteratorToWchc wchc = new IteratorToWchc(
                                    builder,
                                    input, 
                                    SettableFuturePromise.<FourLetterWords.Wchc>create());
                            wchc.run();
                            return wchc;
                        }
                    }, SameThreadExecutor.getInstance());
        final ListenableFuture<FourLetterWords.Wchc> future;
        if (input.isEmpty()) {
            future = wchc;
        } else {
            future = Futures.transform(
                    wchc, 
                    new AsyncFunction<FourLetterWords.Wchc,FourLetterWords.Wchc>() {
                        @Override
                        public ListenableFuture<FourLetterWords.Wchc> apply(FourLetterWords.Wchc input)
                                throws Exception {
                            return CreateWatches.call(input, prefix, labelOf, codec, client, LogManager.getLogger(SnapshotEnsembleWatches.class));
                        }
                    }, SameThreadExecutor.getInstance());
        }
        return Optional.of(future);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).toString();
    }
    
    protected static final class CreateWatches<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<List<O>> implements Runnable, Callable<Optional<FourLetterWords.Wchc>> {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<FourLetterWords.Wchc> call(
                final FourLetterWords.Wchc wchc,
                final ZNodePath prefix,
                final Function<ZNodePath,ZNodeLabel> labelOf,
                final ByteCodec<Object> codec,
                final ClientExecutor<? super Records.Request, O, ?> client,
                final Logger logger) {
            final Promise<FourLetterWords.Wchc> promise = SettableFuturePromise.create();
            final Operations.Requests.Create createSession = Operations.Requests.create();
            // the only watches reported by wchc/wchp are data watches
            Operations.Requests.Create createWatch;
            try {
                createWatch = Operations.Requests.create().setData(codec.toBytes(
                         WatchType.DATA));
            } catch (IOException e) {
                promise.setException(e);
                return promise;
            }
            ImmutableList.Builder<SubmittedRequest<? extends Records.Request, O>> builder = ImmutableList.builder();
            for (Map.Entry<Long, Collection<ZNodePath>> entry: wchc) {
                ZNodePath path = prefix.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Session.labelOf(entry.getKey().longValue()));
                builder.add(SubmittedRequest.submit(client, (createSession.setPath(path).build())));
                for (ZNodePath watch: entry.getValue()) {
                    builder.add(SubmittedRequest.submit(client, createWatch.setPath(path.join(labelOf.apply(watch))).build()));
                }
            }
            new CreateWatches<O>(wchc, builder.build(), promise);
            return promise;
        }
        
        private final FourLetterWords.Wchc wchc;
        private final List<? extends SubmittedRequest<? extends Records.Request, O>> requests;
        private final CallablePromiseTask<?,FourLetterWords.Wchc> delegate;
        
        protected CreateWatches(
                FourLetterWords.Wchc wchc,
                List<? extends SubmittedRequest<? extends Records.Request, O>> requests,
                Promise<FourLetterWords.Wchc> promise) {
            super(Futures.allAsList(requests));
            this.wchc = wchc;
            this.requests = requests;
            this.delegate = CallablePromiseTask.create(this, promise);
            addListener(this, SameThreadExecutor.getInstance());
        }

        @Override
        public void run() {
            delegate.run();
        }

        @Override
        public Optional<Wchc> call() throws Exception {
            if (isDone()) {
                get();
                for (SubmittedRequest<? extends Records.Request, O> request: requests) {
                    Operations.maybeError(request.get().record(), KeeperException.Code.NODEEXISTS);
                }
                return Optional.of(wchc);
            }
            return Optional.absent();
        }
        
        @Override
        protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper helper) {
            return super.toStringHelper(helper.addValue(wchc).addValue(toString(delegate)));
        }
    }
    
    public static final class IteratorToWchc extends PromiseTask<PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>, FourLetterWords.Wchc> implements Runnable {

        private final ImmutableSetMultimap.Builder<Long, ZNodePath> builder;
        
        public IteratorToWchc(
                ImmutableSetMultimap.Builder<Long, ZNodePath> builder,
                PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>> task,
                Promise<FourLetterWords.Wchc> delegate) {
            super(task, delegate);
            this.builder = builder;
            addListener(this, SameThreadExecutor.getInstance());
        }

        @Override
        public synchronized void run() {
            if (!isDone()) {
                while (task().hasNext()) {
                    if (task.peek().isDone()) {
                        Map.Entry<Long, Collection<ZNodePath>> entry;
                        try { 
                            entry = task.next().get();
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        } catch (ExecutionException e) {
                            setException(e);
                            return;
                        }
                        builder.putAll(entry.getKey(), entry.getValue());
                    } else {
                        task.peek().addListener(this, SameThreadExecutor.getInstance());
                        return;
                    }
                }
                set(FourLetterWords.Wchc.fromMultimap(builder.build()));
            } else {
                if (task.hasNext()) {
                    task.peek().cancel(false);
                }
            }
        }
    }

    public static final class QueryEnsembleWatches implements Supplier<ListenableFuture<PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>>> {
    
        public static QueryEnsembleWatches forServers(ImmutableList<QueryServerWatches> servers) {
            return new QueryEnsembleWatches(servers);
        }
        
        private final ImmutableList<QueryServerWatches> servers;
    
        protected QueryEnsembleWatches(ImmutableList<QueryServerWatches> servers) {
            this.servers = servers;
        }
        
        @Override
        public ListenableFuture<PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>> get() {
            ImmutableList.Builder<ListenableFuture<Queue<ValueSessionLookup<Collection<ZNodePath>>>>> futures = ImmutableList.builder();
            for (QueryServerWatches server: servers) {
                futures.add(server.get());
            }
            return Futures.transform(
                    Futures.allAsList(futures.build()),
                    new QueueIterator(),
                    SameThreadExecutor.getInstance());
        }
        
        protected static final class QueueIterator implements Function<List<Queue<ValueSessionLookup<Collection<ZNodePath>>>>, PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>>> {
    
            public QueueIterator() {}
            
            @Override
            public PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>> apply(List<Queue<ValueSessionLookup<Collection<ZNodePath>>>> input) {
                return SessionLookupIterator.forQueues(ImmutableList.copyOf(input));
            }
        }
    }

    public static final class StringToWchc implements Function<String, FourLetterWords.Wchc> {

        public StringToWchc() {}
        
        @Override
        public FourLetterWords.Wchc apply(String input) {
            return FourLetterWords.Wchc.fromString(input);
        }
    }

    public static abstract class FilteredWchc implements Function<FourLetterWords.Wchc, FourLetterWords.Wchc> {

        protected FilteredWchc() {}
        
        @Override
        public FourLetterWords.Wchc apply(FourLetterWords.Wchc input) {
            ImmutableSetMultimap.Builder<Long, ZNodePath> filtered = ImmutableSetMultimap.builder();
            for (Map.Entry<Long, Collection<ZNodePath>> entry: input) {
                filtered.putAll(entry.getKey(), filter(entry));
            }
            return FourLetterWords.Wchc.fromMultimap(filtered.build());
        }
        
        protected abstract Iterable<ZNodePath> filter(Map.Entry<Long, Collection<ZNodePath>> entry);
    }
    
    public static final class SessionLookupIterator extends AbstractIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>> implements PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>> {
        
        public static PeekingIterator<ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>>> forQueues(ImmutableList<Queue<ValueSessionLookup<Collection<ZNodePath>>>> queues) {
            return new SessionLookupIterator(queues);
        }
        
        private final ImmutableList<Queue<ValueSessionLookup<Collection<ZNodePath>>>> queues;
        private final Head head;
        
        protected SessionLookupIterator(
                ImmutableList<Queue<ValueSessionLookup<Collection<ZNodePath>>>> queues) {
            this.queues = queues;
            this.head = new Head(queues);
        }

        @Override
        protected synchronized ListenableFuture<Map.Entry<Long, Collection<ZNodePath>>> computeNext() {
            CallablePromiseTask<Head, Map.Entry<Long, Collection<ZNodePath>>> task = CallablePromiseTask.create(
                    head, 
                    SettableFuturePromise.<Map.Entry<Long, Collection<ZNodePath>>>create());
            boolean empty = true;
            for (Queue<ValueSessionLookup<Collection<ZNodePath>>> q: queues) {
                ValueSessionLookup<Collection<ZNodePath>> head = q.peek();
                if (head != null) {
                    empty = false;
                    head.addListener(task, SameThreadExecutor.getInstance());
                }
            }
            if (empty) {
                task.cancel(true);
                return endOfData();
            } else {
                new CancellationListener<Map.Entry<Long, Collection<ZNodePath>>>(task);
                return task;
            }
        }
        
        protected final class Head implements Callable<Optional<Map.Entry<Long, Collection<ZNodePath>>>> {

            private final Iterator<Queue<ValueSessionLookup<Collection<ZNodePath>>>> nextQueue;
            
            public Head(Iterable<Queue<ValueSessionLookup<Collection<ZNodePath>>>> queues) {
                this.nextQueue = Iterators.cycle(queues);
            }
            
            @Override
            public Optional<Map.Entry<Long, Collection<ZNodePath>>> call() throws Exception {
                synchronized (SessionLookupIterator.this) {
                    boolean empty = true;
                    Queue<ValueSessionLookup<Collection<ZNodePath>>> firstQ = nextQueue.next();
                    Queue<ValueSessionLookup<Collection<ZNodePath>>> q = firstQ;
                    do {
                        ValueSessionLookup<Collection<ZNodePath>> head = q.peek();
                        if (head != null) {
                            empty = false;
                            if (head.isDone() && q.remove(head)) {
                                return Optional.of(Maps.immutableEntry(Long.valueOf(head.get().getSessionId().longValue()), head.value()));
                            }
                        }
                        q = nextQueue.next();
                    } while (q != firstQ);
                    if (empty) {
                        throw new NoSuchElementException();
                    }
                    return Optional.absent();
                }
            }
        }
        
        protected final class CancellationListener<V> extends ForwardingListenableFuture.SimpleForwardingListenableFuture<V> implements Runnable {
            
            protected CancellationListener(ListenableFuture<V> delegate) {
                super(delegate);
                addListener(this, SameThreadExecutor.getInstance());
            }

            @Override
            public void run() {
                if (isCancelled()) {
                    Queue<ListenableFuture<?>> drained = Lists.newLinkedList();
                    synchronized (SessionLookupIterator.this) {
                        for (Queue<? extends ListenableFuture<?>> q: queues) {
                            ListenableFuture<?> next;
                            while ((next = q.poll()) != null) {
                                drained.add(next);
                            }
                        }
                    }
                    ListenableFuture<?> next;
                    while ((next = drained.poll()) != null) {
                        next.cancel(false);
                    }
                }
            }
        }
    }
    
    public static final class QueryServerWatches implements Supplier<ListenableFuture<Queue<ValueSessionLookup<Collection<ZNodePath>>>>> {

        public static QueryServerWatches create(
                Materializer<StorageZNode<?>,?> materializer,
                Function<? super String, ? extends FourLetterWords.Wchc> transformWchc,
                Supplier<? extends ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>> connections) {
            return new QueryServerWatches(materializer, transformWchc, connections);
        }
        
        private final Supplier<? extends ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>> connections;
        private final Function<? super String, ? extends FourLetterWords.Wchc> transformWchc;
        private final TransformWatchSessions transformSessions;
        
        protected QueryServerWatches(
                Materializer<StorageZNode<?>,?> materializer,
                Function<? super String, ? extends FourLetterWords.Wchc> transformWchc,
                Supplier<? extends ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>> connections) {
            this.transformSessions = new TransformWatchSessions(materializer);
            this.transformWchc = transformWchc;
            this.connections = connections;
        }
    
        @Override
        public ListenableFuture<Queue<ValueSessionLookup<Collection<ZNodePath>>>> get() {
            ListenableFuture<FourLetterWords.Wchc> wchc = Futures.transform(
                    FourLetterCommand.callThenClose(
                            FourLetterWord.WCHC, 
                            connections.get(), 
                            SettableFuturePromise.<String>create()),
                    transformWchc,
                    SameThreadExecutor.getInstance());
            return Futures.transform(wchc, 
                    transformSessions,
                    SameThreadExecutor.getInstance());
        }
        
        protected final class TransformWatchSessions implements Function<FourLetterWords.Wchc,Queue<ValueSessionLookup<Collection<ZNodePath>>>> {
    
            private final Materializer<StorageZNode<?>,?> materializer;
            
            public TransformWatchSessions(
                    Materializer<StorageZNode<?>,?> materializer) {
                this.materializer = materializer;
            }
            
            @Override
            public Queue<ValueSessionLookup<Collection<ZNodePath>>> apply(
                    FourLetterWords.Wchc input) {
                Queue<ValueSessionLookup<Collection<ZNodePath>>> lookups = new ArrayDeque<ValueSessionLookup<Collection<ZNodePath>>>(input.asMultimap().asMap().size());
                for (Map.Entry<Long,Collection<ZNodePath>> e: input) {
                    ValueSessionLookup<Collection<ZNodePath>> lookup = ValueSessionLookup.create(
                            e.getValue(), 
                            e.getKey(),
                            materializer);
                    lookups.add(lookup);
                }
                return lookups;
            }
        }
    }
}
