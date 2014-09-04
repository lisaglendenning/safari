package edu.uw.zookeeper.safari.control.volumes;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.SubmittedRequests;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.LoggingServiceListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;

public final class VolumeEntryAcceptors<O extends Operation.ProtocolResponse<?>> extends LoggingServiceListener<Service> implements AsyncFunction<VersionedId, Boolean> {

    public static <O extends Operation.ProtocolResponse<?>> VolumeEntryAcceptors<O> listen(
            final Materializer<ControlZNode<?>,O> materializer,
            final WatchListeners cacheEvents,
            final Service service) {
        final VolumeEntryAcceptors<O> listener = new VolumeEntryAcceptors<O>(
                materializer,
                cacheEvents,
                service);
        Services.listen(listener, service);
        return listener;
    }
    
    private final ConcurrentMap<Identifier, Object> acceptors;
    private final Materializer<ControlZNode<?>,O> materializer;
    private final WatchListeners cacheEvents;
    
    protected VolumeEntryAcceptors(
            Materializer<ControlZNode<?>,O> materializer,
            WatchListeners cacheEvents,
            Service service) {
        super(service);
        this.acceptors = new MapMaker().makeMap();
        this.materializer = materializer;
        this.cacheEvents = cacheEvents;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ListenableFuture<Boolean> apply(
            final VersionedId version) throws Exception {
        switch (delegate().state()) {
        case NEW:
        case STARTING:
        case RUNNING:
            break;
        default:
            return Futures.immediateCancelledFuture();
        }
        Object value = acceptors.get(version.getValue());
        final ListenableFuture<Boolean> future;
        if (value == null) {
            ZNodePath path = ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(version.getValue());
            GetLatestCallback callback = new GetLatestCallback(
                    version,
                    GetLatest.create(
                        path,
                        SubmittedRequests.submit(
                            materializer, 
                            PathToRequests.forRequests(
                                    Operations.Requests.sync(), 
                                    Operations.Requests.getData()).apply(path)),
                        materializer.cache()),
                    SettableFuturePromise.<Boolean>create());
            if (acceptors.putIfAbsent(version.getValue(), callback) != null) {
                return apply(version);
            } else {
                switch (delegate().state()) {
                case NEW:
                case STARTING:
                case RUNNING:
                    LoggingFutureListener.listen(logger, callback);
                    callback.run();
                    break;
                default:
                    acceptors.remove(version.getValue(), callback);
                    callback.promise().cancel(false);
                    break;
                }
                future = callback.promise();
            }
        } else if (value instanceof VolumeEntryAcceptors.GetLatestCallback) {
            GetLatestCallback callback = (GetLatestCallback) value;
            int cmp = callback.version().getVersion().compareTo(version.getVersion());
            if (cmp == 0) {
                future = callback.promise();
            } else if (cmp < 0) {
                GetLatestCallback updated = new GetLatestCallback(
                        version,
                        callback,
                        SettableFuturePromise.<Boolean>create());
                if (!acceptors.replace(version.getValue(), value, updated)) {
                    return apply(version);
                } else {
                    callback.onSuccess(Boolean.FALSE);
                    callback = updated;
                    switch (delegate().state()) {
                    case NEW:
                    case STARTING:
                    case RUNNING:
                        LoggingFutureListener.listen(logger, callback);
                        callback.run();
                        break;
                    default:
                        acceptors.remove(version.getValue(), callback);
                        callback.promise().cancel(false);
                        break;
                    }
                    future = callback.promise();
                }
            } else {
                future = Futures.immediateFuture(Boolean.FALSE);
            }
        } else {
            Renewer renewer = (Renewer) value;
            int cmp = renewer.version().getVersion().compareTo(version.getVersion());
            if (cmp == 0) {
                future = renewer.get();
            } else if (cmp < 0) {
                Renewer updated = new Renewer(
                        VolumeEntryAcceptor.defaults(
                                version, 
                                materializer, 
                                cacheEvents));
                if (!acceptors.replace(renewer.version().getValue(), renewer, updated)) {
                    return apply(version);
                } else {
                    future = updated.get();
                    switch (delegate().state()) {
                    case NEW:
                    case STARTING:
                    case RUNNING:
                        break;
                    default:
                        acceptors.remove(version.getValue(), updated);
                        future.cancel(false);
                        break;
                    }
                }
            } else {
                future = Futures.immediateFuture(Boolean.FALSE);
            }
        }
        return future;
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        stop();
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        Services.stop(delegate());
        stop();
    }
    
    protected void stop() {
        for (Object value: Iterables.consumingIterable(acceptors.values())) {
            if (value instanceof VolumeEntryAcceptors.GetLatestCallback) {
                ((VolumeEntryAcceptors<?>.GetLatestCallback) value).cancel(false);
            }
        }
    }
    
    protected final class GetLatestCallback extends SimpleToStringListenableFuture<UnsignedLong> implements Runnable, FutureCallback<Boolean> {
        
        private final VersionedId version;
        private final Promise<Boolean> promise;
        
        protected GetLatestCallback(
                VersionedId version,
                ListenableFuture<UnsignedLong> future,
                Promise<Boolean> promise) {
            super(future);
            this.version = version;
            this.promise = promise;
        }
        
        public VersionedId version() {
            return version;
        }
        
        public Promise<Boolean> promise() {
            return promise;
        }

        @Override
        public void run() {
            if (isDone()) {
                if (isCancelled()) {
                    promise.cancel(false);
                    return;
                }
                UnsignedLong latest;
                try { 
                    latest = get();
                } catch (ExecutionException e) {
                    onFailure(e);
                    return;
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
                if (latest.equals(version.getVersion())) {
                    Renewer renewer = new Renewer(
                            VolumeEntryAcceptor.defaults(
                                    version, 
                                    materializer, 
                                    cacheEvents));
                    if (acceptors.replace(version.getValue(), this, renewer)) {
                        switch (VolumeEntryAcceptors.this.delegate().state()) {
                        case NEW:
                        case STARTING:
                        case RUNNING:
                            if (!promise.isDone()) {
                                Futures.addCallback(renewer.get(), this, SameThreadExecutor.getInstance());
                            }
                            break;
                        default:
                            acceptors.remove(version.getValue(), renewer);
                            promise.cancel(false);
                            break;
                        }
                    } else {
                        // must be shutting down
                        promise.cancel(false);
                    }
                } else {
                    assert (latest.longValue() > version.getVersion().longValue());
                    onSuccess(Boolean.FALSE);
                }
            } else {
                addListener(this, SameThreadExecutor.getInstance());
            }
        }

        @Override
        public void onSuccess(Boolean result) {
            if (!promise.isDone()) {
                promise.set(result);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (!promise.isDone()) {
                promise.setException(t);
            }
        }
    }
    
    protected static final class GetLatest<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<List<O>, UnsignedLong> {

        public static <O extends Operation.ProtocolResponse<?>> ListenableFuture<UnsignedLong> create(
                ZNodePath path,
                ListenableFuture<List<O>> future,
                LockableZNodeCache<ControlZNode<?>,?,O> cache) {
            return Futures.transform(future, new GetLatest<O>(path, cache), SameThreadExecutor.getInstance());
        }
        
        private final LockableZNodeCache<ControlZNode<?>,?,?> cache;
        private final ZNodePath path;
        
        protected GetLatest(
                ZNodePath path,
                LockableZNodeCache<ControlZNode<?>,?,?> cache) {
            this.path = path;
            this.cache = cache;
        }
        
        @Override
        public ListenableFuture<UnsignedLong> apply(List<O> input) throws Exception {
            for (O response: input) {
                Operations.unlessError(response.record());
            }
            cache.lock().readLock().lock();
            try {
                return Futures.immediateFuture((UnsignedLong) cache.cache().get(path).data().get());
            } finally {
                cache.lock().readLock().unlock();
            }
        }
    }
    
    protected static final class Renewer implements AsyncFunction<Object, Boolean>, Supplier<ListenableFuture<Boolean>> {
        
        private final VolumeEntryAcceptor acceptor;
        
        protected Renewer(VolumeEntryAcceptor acceptor) {
            this.acceptor = acceptor;
        }
        
        public VersionedId version() {
            return acceptor.version();
        }
        
        @Override
        public ListenableFuture<Boolean> get() {
            return Futures.transform(
                    acceptor.get(), 
                    this, 
                    SameThreadExecutor.getInstance());
        }
        
        @Override
        public ListenableFuture<Boolean> apply(Object input)
                throws Exception {
            if (input instanceof Optional) {
                return Futures.immediateFuture(Boolean.valueOf(!((Optional<?>) input).isPresent()));
            } else {
                UnsignedLong latest = (UnsignedLong) input;
                if (latest.equals(version().getVersion())) {
                    return get();
                } else {
                    return Futures.immediateFuture(Boolean.FALSE);
                }
            }
        }
    }
}
