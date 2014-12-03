package edu.uw.zookeeper.safari.storage.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.FutureChain.FutureListChain;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.AbstractZNodeLabel;
import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.RelativeZNodePath;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Assumes all ephemerals have already been committed.
 */
public final class RecreateWatches<O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request,?,?> & Connection.Listener<? super Operation.Response>> extends RecreateSessionValues<T,StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch> {

    public static <O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request,?,?> & Connection.Listener<? super Operation.Response>> ListenableFuture<ZNodePath> listen(
            ZNodePath snapshot,
            Function<? super ZNodeName, ? extends ZNodePath> toPath,
            Function<? super Long, ? extends T> executors,
            Materializer<StorageZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        final FutureCallback<?> callback = Watchers.StopServiceOnFailure.create(service);
        final RecreateWatches<O,T> instance = create(snapshot, toPath, callback, materializer, executors);
        final Promise<ZNodePath> committed = committed(instance, materializer.cache(), cacheEvents, logger);
        ImmutableList<? extends WatchMatchServiceListener> listeners = listeners(instance, materializer, cacheEvents, service, logger);
        listen(listeners, committed, service);
        return committed;
    }
    
    protected static <O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request,?,?> & Connection.Listener<? super Operation.Response>> RecreateWatches<O,T> create(
            ZNodePath snapshot,
            Function<? super ZNodeName, ? extends ZNodePath> toPath,
            FutureCallback<?> callback,
            Materializer<StorageZNode<?>,O> materializer,
            Function<? super Long, ? extends T> executors) {
        // not threadsafe
        @SuppressWarnings("unchecked")
        final ImmutableMap<Watcher.WatcherType, Function<ZNodePath,List<Records.Request>>> requests = 
                ImmutableMap.<Watcher.WatcherType, Function<ZNodePath,List<Records.Request>>>of(
                Watcher.WatcherType.Data, 
                PathToRequests.forRequests(Operations.Requests.sync(), Operations.Requests.exists().setWatch(true)));
        return new RecreateWatches<O,T>(
                toPath, 
                WatchName.create(materializer.cache(), materializer),
                Functions.forMap(requests), 
                snapshot,
                materializer,
                callback, 
                executors);
    }

    private final Function<? super ZNodeName, ? extends ZNodePath> toPath;
    private final AsyncFunction<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch, ZNodeName> toName;
    private final Function<Watcher.WatcherType, Function<ZNodePath, List<Records.Request>>> requests;
    private final Function<? super ZNodePath, ? extends Records.Request> commit;
    private final Materializer<StorageZNode<?>,O> materializer;
    private final ConcurrentMap<ZNodePath, CommitWatch> recreates;
    
    protected RecreateWatches(
            Function<? super ZNodeName, ? extends ZNodePath> toPath,
            AsyncFunction<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch, ZNodeName> toName,
            Function<Watcher.WatcherType, Function<ZNodePath, List<Records.Request>>> requests,
            ZNodePath snapshot,
            final Materializer<StorageZNode<?>,O> materializer,
            FutureCallback<?> callback,
            Function<? super Long, ? extends T> executors) {
        super(snapshot,
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch.class, 
                materializer.schema(),
                callback, 
                executors);
        this.materializer = materializer;
        this.toPath = toPath;
        this.toName = toName;
        this.requests = requests;
        // threadsafe
        try {
            this.commit = new Function<ZNodePath, Records.Request>() {
                final Operations.Requests.Create create = Operations.Requests.create().setData(materializer.codec().toBytes(Boolean.TRUE));
                @Override
                public synchronized Records.Request apply(ZNodePath input) {
                    return create.setPath(input.join(StorageZNode.CommitZNode.LABEL)).build();
                }
            };
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        this.recreates = new MapMaker().makeMap();
    }
    
    @Override
    public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch watch) {
        if ((watch.commit() != null) || (watch.data().stamp() < 0L)) {
            return;
        }
        Long id = Long.valueOf(((StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values) watch.parent().get()).session().name().longValue());
        T executor = getExecutor(id);
        if (executor == null) {
            return;
        }
        new CommitWatch(id, watch).run();
    }
    
    protected final class RecreateWatch extends ToStringListenableFuture<Boolean> implements Runnable, ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>> {

        private final Long id;
        private final ZNodePath path;
        private final Watcher.WatcherType type;
        private final ChainedFutures.ChainedFuturesTask<Boolean> delegate;
        
        protected RecreateWatch(
                Long id,
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch watch) {
            this.id = id;
            this.path = watch.path();
            this.type = watch.data().get();
            this.delegate = ChainedFutures.task(
                    ChainedFutures.<Boolean>castLast(
                            ChainedFutures.arrayList(this, 3)));
        }
        
        public ZNodePath path() {
            return path;
        }
        
        @Override
        public void run() {
            delegate.run();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                ListenableFuture<ZNodeName> future;
                materializer.cache().lock().readLock().lock();
                try {
                    future = toName.apply((StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch) materializer.cache().cache().get(path));
                } finally {
                    materializer.cache().lock().readLock().unlock();
                }
                return Optional.of(future);
            }
            case 1:
            {
                ZNodeName name;
                try {
                    name = (ZNodeName) input.getLast().get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KeeperException.NoNodeException) {
                        // sequential ephemeral node has expired,
                        // inject delete notification with original label
                        materializer.cache().lock().readLock().lock();
                        try {
                            StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch watch = getWatch();
                            name = ZNodeName.fromString(watch.name());
                        } finally {
                            materializer.cache().lock().readLock().unlock();
                        }
                        ZNodePath path = toPath.apply(name);
                        injectDeleted(path);
                        return Optional.of(Futures.immediateFuture(Boolean.FALSE));
                    } else {
                        throw e;
                    }
                }
                ZNodePath path = toPath.apply(name);
                return Optional.of(
                        SubmittedRequests.submit(
                                getExecutor(), 
                                requests.apply(type).apply(path)));
            }
            case 2:
            {
                Object last = input.getLast().get();
                if (last instanceof Boolean) {
                    return Optional.absent();
                }
                Optional<Operation.Error> error = Optional.absent();
                for (Operation.ProtocolResponse<?> response: (List<? extends Operation.ProtocolResponse<?>>) last) {
                    error = Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                }
                Boolean success = Boolean.TRUE;
                if (error.isPresent()) {
                    boolean ephemeral;
                    materializer.cache().lock().readLock().lock();
                    try {
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch watch = getWatch();
                        ephemeral = (watch.ephemeral() != null);
                    } finally {
                        materializer.cache().lock().readLock().unlock();
                    }
                    if (ephemeral) {
                        // this watch was pointing to an ephemeral that is now expired
                        List<Records.Request> requests = ((SubmittedRequests<Records.Request,?>) input.getLast()).getValue();
                        ZNodePath path = ZNodePath.fromString(((Records.PathGetter) requests.get(requests.size()-1)).getPath());
                        injectDeleted(path);
                        success = Boolean.FALSE;
                    }
                }
                return Optional.of(Futures.immediateFuture(success));
            }
            default:
                break;
            }
            return Optional.absent();
        }
        
        public T getExecutor() throws KeeperException {
            T executor = RecreateWatches.this.getExecutor(id);
            if (executor == null) {
                throw new KeeperException.SessionMovedException();
            }
            return executor;
        }
        
        // assumes cache is read locked
        protected StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch getWatch() {
            return (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch) materializer.cache().cache().get(path);
        }
        
        protected void injectDeleted(ZNodePath path) throws KeeperException {
            injectNotification(NodeWatchEvent.nodeDeleted(path));
        }
        
        protected void injectNotification(WatchEvent event) throws KeeperException {
            getExecutor().handleConnectionRead(event.toMessage());
        }

        @Override
        protected ListenableFuture<Boolean> delegate() {
            return delegate;
        }
    }
    
    protected final class CommitWatch extends ToStringListenableFuture<Boolean> implements Runnable, ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>> {

        private final RecreateWatch recreate;
        private final ChainedFutures.ChainedFuturesTask<Boolean> delegate;
        
        protected CommitWatch(
                Long id,
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch watch) {
            this.recreate = new RecreateWatch(id, watch);
            this.delegate = ChainedFutures.task(
                    ChainedFutures.<Boolean>castLast(
                            ChainedFutures.arrayList(this, 3)));
        }

        @Override
        public void run() {
            if (isDone()) {
                if (recreates.remove(recreate.path(), this)) {
                    try {
                        get();
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }
            } else {
                if (recreates.putIfAbsent(recreate.path(), this) == null) {
                    addListener(this, MoreExecutors.directExecutor());
                    delegate.run();
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                recreate.run();
                return Optional.of(recreate);
            }
            case 1:
            {
                try {
                    input.getLast().get();
                } catch (ExecutionException e) {
                    if ((e.getCause() instanceof KeeperException.SessionMovedException) ||
                            (e.getCause() instanceof KeeperException.SessionExpiredException)) {
                        // Note we assume that this exception is coming from the client session
                        return Optional.of(Futures.immediateFuture(Boolean.FALSE));
                    }
                    throw e;
                }
                return Optional.of(materializer.submit(commit.apply(recreate.path())));
            }
            case 2:
            {
                Object last = input.getLast().get();
                if (last instanceof Boolean) {
                    return Optional.absent();
                }
                Operations.maybeError(((O) last).record(), KeeperException.Code.NODEEXISTS);
                return Optional.of(Futures.immediateFuture(Boolean.TRUE));
            }
            default:
                break;
            }
            return Optional.absent();
        }
        
        @Override
        protected ListenableFuture<Boolean> delegate() {
            return delegate;
        }
    }
    
    protected static final class WatchName<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch, ZNodeName> {

        public static <O extends Operation.ProtocolResponse<?>> WatchName<O> create(
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                ClientExecutor<? super Records.Request, O, ?> client) {
            return new WatchName<O>(cache, client);
        }
        
        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final PathToQuery<?,O> getSuffix;

        @SuppressWarnings("unchecked")
        protected WatchName(
                LockableZNodeCache<StorageZNode<?>,?,?> cache,
                ClientExecutor<? super Records.Request, O, ?> client) {
            this.cache = cache;
            this.getSuffix = PathToQuery.forFunction(
                    client, 
                    Functions.compose(
                            PathToRequests.forRequests(Operations.Requests.sync(), Operations.Requests.getData()),
                            JoinToPath.forName(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral.Sequence.LABEL)));
        }

        /**
         * Not threadsafe.
         */
        @Override
        public ListenableFuture<ZNodeName> apply(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Watches.Sessions.Session.Values.Watch watch) {
            ZNodeName name = ZNodeName.fromString(watch.name());
            if (watch.ephemeral() != null) {
                AbstractZNodeLabel label = (name instanceof AbstractZNodeLabel) ? (AbstractZNodeLabel) name : Iterables.getLast(((RelativeZNodePath) name));
                Optional<? extends Sequential<String,?>> sequential = Sequential.maybeFromString(label.toString());
                if (sequential.isPresent()) {
                    ZNodeName prefix = (name instanceof RelativeZNodePath) ?
                            ((RelativeZNodePath) name).prefix(name.length() - label.length() - 1) : 
                                EmptyZNodeLabel.getInstance();
                    ZNodePath path = watch.path();
                    for (int i=0; i<5; ++i) {
                        path = ((AbsoluteZNodePath) path).parent();
                    }
                    path = path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.LABEL)
                            .join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.LABEL)
                            .join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.labelOf(watch.ephemeral().data().get().longValue()))
                            .join(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.LABEL)
                            .join(watch.parent().name());
                    return Futures.transform(
                                Futures.transform(
                                        Futures.allAsList(getSuffix.apply(path).call()),
                                        new GetEphemeralSequence(path)), 
                                new GetSequentialLabel(
                                        prefix,
                                        sequential.get()));
                }
            }
            return Futures.immediateFuture(name);
        }   
        
        protected final class GetEphemeralSequence implements AsyncFunction<List<O>,Number> {

            private final ZNodePath path;
            
            protected GetEphemeralSequence(
                    ZNodePath path) {
                this.path = path;
            }
            
            @Override
            public ListenableFuture<Number> apply(List<O> input) throws Exception {
                for (O response: input) {
                    Operations.unlessError(response.record());
                }
                Number result;
                cache.lock().readLock().lock();
                try {
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral.Sequence sequence = ((StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Ephemerals.Sessions.Session.Values.Ephemeral) cache.cache().get(path)).sequence();
                    result = sequence.data().get();
                } finally {
                    cache.lock().readLock().unlock();
                }
                return Futures.immediateFuture(result);
            }
        }
        
        protected final class GetSequentialLabel implements Function<Number,ZNodeName> {

            private final ZNodeName prefix;
            private final Sequential<String,?> sequential;
            
            protected GetSequentialLabel(
                    ZNodeName prefix,
                    Sequential<String,?> sequential) {
                this.prefix = prefix;
                this.sequential = sequential;
                
            }
            
            @Override
            public ZNodeName apply(Number input) {
                return ZNodeName.fromString(ZNodePath.join(prefix.toString(), Sequential.fromInt(sequential.prefix(), input.intValue()).toString()));
            }
        }
    }
}
