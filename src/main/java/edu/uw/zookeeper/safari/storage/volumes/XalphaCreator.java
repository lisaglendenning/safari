package edu.uw.zookeeper.safari.storage.volumes;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ChainedFutures.ChainedFuturesTask;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.StampedValue;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.VersionedId;
import edu.uw.zookeeper.safari.VersionedValue;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Creates Xalpha for a volume version when it is the latest version and it has no entries.
 */
public final class XalphaCreator<O extends Operation.ProtocolResponse<?>> extends Watchers.StopServiceOnFailure<ZNodePath, Service> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {

        protected Module() {}
        
        @Provides @Singleton
        public XalphaCreator<?> getXalphaCreator(
                final @Volumes Function<Identifier, FutureTransition<UnsignedLong>> latest,
                final VolumeVersionCache.CachedVolumes volumes,
                final @Volumes AsyncFunction<VersionedId, Optional<VersionedId>> precedingVersion,
                final DirectoryEntryListener<StorageZNode<?>, StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> client,
                final RegionRoleService service) {
            return XalphaCreator.listen(
                    latest, 
                    precedingVersion, 
                    volumes, 
                    client, 
                    versions, 
                    service);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(XalphaCreator.class);
        }

        @Override
        protected void configure() {
            bind(XalphaCreator.class).to(new TypeLiteral<XalphaCreator<?>>(){});
        }
    }
    
    public static <O extends Operation.ProtocolResponse<?>> XalphaCreator<O> listen(
            final Function<Identifier, FutureTransition<UnsignedLong>> latestVersion,
            final AsyncFunction<VersionedId, Optional<VersionedId>> precedingVersion,
            final VolumeVersionCache.CachedVolumes volumes,
            final SchemaClientService<StorageZNode<?>,O> client,
            final DirectoryEntryListener<StorageZNode<?>, StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
            final Service service) {
        return listen(
                latestVersion, 
                precedingVersion, 
                GetXomega.create(volumes), 
                client, 
                versions, 
                service);
    }
    
    public static <O extends Operation.ProtocolResponse<?>> XalphaCreator<O> listen(
            final Function<Identifier, FutureTransition<UnsignedLong>> latestVersion,
            final AsyncFunction<VersionedId, Optional<VersionedId>> precedingVersion,
            final AsyncFunction<VersionedId, Long> getXomega,
            final SchemaClientService<StorageZNode<?>,O> client,
            final DirectoryEntryListener<StorageZNode<?>, StorageSchema.Safari.Volumes.Volume.Log.Version> versions,
            final Service service) {
        final XalphaCreator<O> instance = new XalphaCreator<O>(
                latestVersion, 
                UseXomega.create(client.materializer().cache(), precedingVersion), 
                getXomega, 
                client.materializer(), 
                service);
        final Watchers.EventToPathCallback<VersionWatcher<O>> versionWatcher = 
                Watchers.EventToPathCallback.create(
                        VersionWatcher.create(instance, client.materializer()));
        Watchers.FutureCallbackServiceListener.listen(
                versionWatcher, 
                service, 
                client.notifications(), 
                WatchMatcher.exact(
                        versions.schema().path(), 
                        Watcher.Event.EventType.NodeChildrenChanged), 
                instance.logger());
        final Watchers.EventToPathCallback<XalphaCreator<O>> listener = 
                Watchers.EventToPathCallback.create(instance);
        versions.add(Watchers.FutureCallbackListener.create(
                listener, 
                WatchMatcher.exact(
                        versions.schema().path(), 
                        Watcher.Event.EventType.NodeChildrenChanged,
                        Watcher.Event.EventType.NodeDeleted), 
                instance.logger()));
        versions.add(Watchers.FutureCallbackListener.create(
                versionWatcher, 
                WatchMatcher.exact(
                        versions.schema().path(), 
                        Watcher.Event.EventType.NodeCreated), 
                instance.logger()));
        final Service.Listener unsubscriber = new Service.Listener() {
            @Override
            public void terminated(Service.State from) {
                unsubscribe();
            }
            
            @Override
            public void failed(Service.State from, Throwable failure) {
                unsubscribe();
            }
            
            public void unsubscribe() {
                versions.getWatcher().getListeners().remove(listener);
                versions.getWatcher().getListeners().remove(versionWatcher);
            }
        };
        service.addListener(unsubscriber, MoreExecutors.directExecutor());
        switch (service.state()) {
        case TERMINATED:
            unsubscriber.terminated(Service.State.RUNNING);
            break;
        case FAILED:
            unsubscriber.failed(Service.State.RUNNING, service.failureCause());
            break;
        default:
            break;
        }
        return instance;
    }
    
    private final Materializer<StorageZNode<?>, O> materializer;
    private final Function<Identifier, FutureTransition<UnsignedLong>> latestVersion;
    private final AsyncFunction<VersionedId, Boolean> useXomega;
    private final AsyncFunction<VersionedId, Long> getXomega;
    private final ConcurrentMap<VersionedId, VersionCallback> callbacks;
    
    protected XalphaCreator(
            Function<Identifier, FutureTransition<UnsignedLong>> latestVersion,
            AsyncFunction<VersionedId, Boolean> useXomega,
            AsyncFunction<VersionedId, Long> getXomega,
            Materializer<StorageZNode<?>, O> materializer,
            Service service) {
        super(service);
        this.materializer = materializer;
        this.useXomega = useXomega;
        this.getXomega = getXomega;
        this.latestVersion = latestVersion;
        this.callbacks = new MapMaker().makeMap();
    }

    @Override
    public void onSuccess(ZNodePath result) {
        VersionCallback callback = null;
        VersionedId version = null;
        materializer.cache().lock().readLock().lock();
        try {
            StorageSchema.Safari.Volumes.Volume.Log.Version node = (StorageSchema.Safari.Volumes.Volume.Log.Version) materializer.cache().cache().get(result);
            if (node != null) {
                callback = callbacks.get(node.id());
                if (callback == null) {
                    if (node.isEmpty() && (node == node.log().versions().lastEntry().getValue())) {
                        version = node.id();
                    }
                }
            } else {
                UnsignedLong v = UnsignedLong.valueOf(result.label().toString());
                ZNodePath path = ((AbsoluteZNodePath) result).parent();
                StorageSchema.Safari.Volumes.Volume.Log log = (StorageSchema.Safari.Volumes.Volume.Log) materializer.cache().cache().get(path);
                Identifier id;
                if (log != null) {
                    id = log.volume().name();
                } else {
                    id = Identifier.valueOf(path.label().toString());
                }
                callback = callbacks.remove(VersionedId.valueOf(v, id));
            }
        } finally {
            materializer.cache().lock().readLock().unlock();
        }
        if (version != null) {
            callback = new VersionCallback(version, result);
            VersionCallback existing = callbacks.putIfAbsent(version, callback);
            assert (existing == null);
        } 
        if (callback != null) {
            callback.run();
        }
    }
    
    public static final class GetChildrenQuery<O extends Operation.ProtocolResponse<?>> implements AsyncFunction<Pair<ZNodePath, ? extends FutureCallback<? super Optional<StampedValue<List<String>>>>>, List<O>> {

        @SuppressWarnings("unchecked")
        public static <O extends Operation.ProtocolResponse<?>> GetChildrenQuery<O> create(
                ClientExecutor<? super Records.Request, O, ?> client) {
            return new GetChildrenQuery<O>(PathToQuery.forRequests(
                    client, 
                    Operations.Requests.sync(), 
                    Operations.Requests.getChildren().setWatch(true)), 
                    new GetChildren());
        }
        
        protected final PathToQuery<?,O> query;
        protected final Processor<? super List<O>, Optional<StampedValue<List<String>>>> processor;
        
        protected GetChildrenQuery(
                PathToQuery<?,O> query,
                Processor<? super List<O>, Optional<StampedValue<List<String>>>> processor) {
            this.query = query;
            this.processor = processor;
        }

        @Override
        public Watchers.Query<O,Optional<StampedValue<List<String>>>> apply(Pair<ZNodePath, ? extends FutureCallback<? super Optional<StampedValue<List<String>>>>> input) {
            Watchers.Query<O,Optional<StampedValue<List<String>>>> future = Watchers.Query.call(
                    processor,
                    input.second(),
                    query.apply(input.first()));
            future.run();
            return future;
        }
        
        public static final class GetChildren implements Processor<List<? extends Operation.ProtocolResponse<?>>, Optional<StampedValue<List<String>>>> {

            private final KeeperException.Code[] codes;
            
            public GetChildren() {
                this(KeeperException.Code.NONODE);
            }
            
            public GetChildren(KeeperException.Code...codes) {
                this.codes = codes;
            }
            
            @Override
            public Optional<StampedValue<List<String>>> apply(
                    List<? extends Operation.ProtocolResponse<?>> input) throws Exception {
                for (Operation.ProtocolResponse<?> response: input) {
                    if (!Operations.maybeError(response.record(), codes).isPresent()) {
                        if (response.record() instanceof Records.ChildrenGetter) {
                            return Optional.of(StampedValue.valueOf(response.zxid(), ((Records.ChildrenGetter) response.record()).getChildren()));
                        }
                    }
                }
                return Optional.absent();
            }
        }
    }
    
    protected static final class UseXomega implements AsyncFunction<VersionedId, Boolean> {
    
        /**
         * Assumes that the preceding resident version is cached if it exists.
         */
        public static UseXomega create(
                final LockableZNodeCache<StorageZNode<?>,?,?> cache,
                final AsyncFunction<VersionedId, Optional<VersionedId>> precedingVersion) {
            final Predicate<VersionedId> isResident = new Predicate<VersionedId>() {
                @Override
                public boolean apply(VersionedId input) {
                    cache.lock().readLock().lock();
                    try {
                        return cache.cache().containsKey(StorageSchema.Safari.Volumes.Volume.Log.Version.pathOf(input.getValue(), input.getVersion()));
                    } finally {
                        cache.lock().readLock().unlock();
                    }
                }
            };
            return new UseXomega(isResident, precedingVersion);
        }
        
        public static UseXomega create(
                final Predicate<VersionedId> isResident,
                final AsyncFunction<VersionedId, Optional<VersionedId>> precedingVersion) {
            return new UseXomega(isResident, precedingVersion);
        }
        
        private final AsyncFunction<VersionedId, Optional<VersionedId>> precedingVersion;
        private final Predicate<VersionedId> isResident;
        
        protected UseXomega(
                Predicate<VersionedId> isResident,
                AsyncFunction<VersionedId, Optional<VersionedId>> precedingVersion) {
            this.precedingVersion = precedingVersion;
            this.isResident = isResident;
        }
    
        @Override
        public ListenableFuture<Boolean> apply(final VersionedId input)
                throws Exception {
            return CallablePromiseTask.listen(
                    new PrecedingIsResident(precedingVersion.apply(input)), 
                        SettableFuturePromise.<Boolean>create());
        }
        
        private final class PrecedingIsResident extends SimpleToStringListenableFuture<Optional<VersionedId>> implements Callable<Optional<Boolean>> {
            
            private PrecedingIsResident(
                    ListenableFuture<Optional<VersionedId>> delegate) {
                super(delegate);
            }
    
            @Override
            public Optional<Boolean> call() throws Exception {
                if (isDone()) {
                    final Optional<VersionedId> preceding = get();
                    final boolean result = preceding.isPresent() ? isResident.apply(preceding.get()) : false;
                    return Optional.of(Boolean.valueOf(result));
                }
                return Optional.absent();
            }
        }
    }
    
    protected static final class GetXomega implements AsyncFunction<VersionedId, Long> {

        public static GetXomega create(
                VolumeVersionCache.CachedVolumes volumes) {
            return new GetXomega(volumes);
        }
        
        private final VolumeVersionCache.CachedVolumes volumes;
        
        protected GetXomega(
                VolumeVersionCache.CachedVolumes volumes) {
            this.volumes = volumes;
        }
        
        @Override
        public ListenableFuture<Long> apply(VersionedId input) throws Exception {
            VolumeVersionCache.CachedVolume volume = volumes.apply(input.getValue());
            ListenableFuture<Pair<? extends VolumeVersionCache.Version, VersionedValue<VolumeVersionCache.VersionState>>> future;
            volume.getLock().readLock().lock();
            try {
                VolumeVersionCache.Version version = volume.get(input.getVersion());
                if ((version != null) && (version instanceof VolumeVersionCache.PastVersion)) {
                    return Futures.immediateFuture(((VolumeVersionCache.PastVersion) version).getZxids().upperEndpoint());
                }
                future = volume.getFuture();
            } finally {
                volume.getLock().readLock().unlock();
            }
            return Futures.transform(future, new Callback(input));
        }
        
        private final class Callback implements AsyncFunction<Object, Long> {
            private final VersionedId version;
            
            private Callback(VersionedId version) {
                this.version = version;
            }

            @Override
            public ListenableFuture<Long> apply(Object input) throws Exception {
                return GetXomega.this.apply(version);
            }
        }
    }

    protected static final class VersionWatcher<O extends Operation.ProtocolResponse<?>> implements FutureCallback<ZNodePath> {

        protected static <O extends Operation.ProtocolResponse<?>> VersionWatcher<O> create(
                FutureCallback<? super ZNodePath> callback,
                Materializer<StorageZNode<?>,O> materializer) {
            return new VersionWatcher<O>(GetChildrenQuery.create(materializer), callback, materializer.cache());
        }

        private final LockableZNodeCache<StorageZNode<?>,?,?> cache;
        private final GetChildrenQuery<O> query;
        private final FutureCallback<? super ZNodePath> callback;
        
        protected VersionWatcher(
                GetChildrenQuery<O> query,
                FutureCallback<? super ZNodePath> callback,
                LockableZNodeCache<StorageZNode<?>,?,?> cache) {
            this.query = query;
            this.callback = callback;
            this.cache = cache;
        }
        
        @Override
        public void onSuccess(ZNodePath result) {
            cache.lock().readLock().lock();
            try {
                StorageSchema.Safari.Volumes.Volume.Log.Version version = (StorageSchema.Safari.Volumes.Volume.Log.Version) cache.cache().get(result);
                if (version != null) {
                    if (version != version.log().versions().lastEntry().getValue()) {
                        return;
                    }
                } else {
                    StorageSchema.Safari.Volumes.Volume.Log log = (StorageSchema.Safari.Volumes.Volume.Log) cache.cache().get(((AbsoluteZNodePath) result).parent());
                    Map.Entry<ZNodeName, StorageSchema.Safari.Volumes.Volume.Log.Version> last = log.versions().lastEntry();
                    if ((last != null) && (last.getValue().name().longValue() > UnsignedLong.valueOf(result.label().toString()).longValue())) {
                        return;
                    }
                }
            } finally {
                cache.lock().readLock().unlock();
            }
            query.apply(Pair.create(result, new Callback(result)));
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
        
        protected final class Callback implements FutureCallback<Optional<StampedValue<List<String>>>> {
            
            private final ZNodePath path;
            
            protected Callback(ZNodePath path) {
                this.path = path;
            }

            @Override
            public void onSuccess(Optional<StampedValue<List<String>>> result) {
                if (result.isPresent() && result.get().get().isEmpty()) {
                    callback.onSuccess(path);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                VersionWatcher.this.onFailure(t);
            }
        }
    }
    
    protected final class VersionCallback extends ToStringListenableFuture<Long> implements Runnable {
        
        private final VersionedId version;
        private final ZNodePath path;
        private final IsEmpty<?> isEmpty;
        private final ChainedFuturesTask<Long> delegate;
        
        protected VersionCallback(
                VersionedId version, ZNodePath path) {
            this.version = version;
            this.path = path;
            this.isEmpty = IsEmpty.create(path, materializer);
            this.delegate = ChainedFutures.run(
                    ChainedFutures.<Long>castLast(
                            ChainedFutures.arrayDeque(
                                    new Chained())));
            addListener(this, MoreExecutors.directExecutor());
            isEmpty.addListener(this, MoreExecutors.directExecutor());
        }

        @Override
        public void run() {
            if (isDone()) {
                if (!isEmpty.isDone()) {
                    isEmpty.cancel(false);
                }
                callbacks.remove(version, this);
                return;
            }
            if (isEmpty.isDone()) {
                if (isEmpty.isCancelled()) {
                    delegate().cancel(false);
                    return;
                }
                StampedValue<Boolean> empty;
                try {
                    empty = isEmpty.get();
                } catch (ExecutionException e) {
                    delegate().setException(e);
                    return;
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                }
                if (!empty.get().booleanValue()) {
                    delegate().cancel(false);
                    return;
                }
            } else {
                StampedValue<Boolean> empty = null;
                try {
                    materializer.cache().lock().readLock().lock();
                    try {
                        StorageSchema.Safari.Volumes.Volume.Log.Version node = (StorageSchema.Safari.Volumes.Volume.Log.Version) materializer.cache().cache().get(path);
                        if ((node != null) && (node == node.log().versions().lastEntry().getValue())) {
                            if (!node.isEmpty()) {
                                if (node.snapshot() == null) {
                                    empty = StampedValue.valueOf(node.stamp(), Boolean.FALSE);
                                } 
                            } else {
                                empty = StampedValue.valueOf(node.stamp(), Boolean.TRUE);
                            }
                        } else {
                            throw new CancellationException();
                        }
                    } finally {
                        materializer.cache().lock().readLock().unlock();
                    }
                } catch (CancellationException e) {
                    isEmpty.cancel(false);
                }
                if (!isEmpty.isDone() && (empty != null)) {
                    if (empty.get().booleanValue()) {
                        synchronized (delegate) {
                            FutureChain<? extends ListenableFuture<?>> futures = delegate.task().chain();
                            if (!futures.isEmpty() && (isEmpty == futures.getLast())) {
                                isEmpty.delegate().set(empty);
                            }
                        }
                    } else {
                        isEmpty.delegate().set(empty);
                    }
                }
            }
            delegate.run();
        }

        @Override
        protected Promise<Long> delegate() {
            return delegate;
        }
        
        protected final class Chained implements ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureDequeChain<ListenableFuture<?>>> {

            protected Chained() {}
            
            @Override
            public Optional<? extends ListenableFuture<?>> apply(
                    FutureChain.FutureDequeChain<ListenableFuture<?>> input) throws Exception {
                if (input.isEmpty()) {
                    return Optional.of(IsLatest.create(version, latestVersion));
                }
                Iterator<ListenableFuture<?>> previous = input.descendingIterator();
                ListenableFuture<?> last = previous.next();
                if (last instanceof IsLatest) {
                    ListenableFuture<?> future;
                    if (!((Boolean) last.get()).booleanValue()) {
                        future = Futures.immediateCancelledFuture();
                    } else {
                        if (!isEmpty.isDone()) {
                            isEmpty.call();
                        }
                        future = isEmpty;
                    }
                    return Optional.of(future);
                } else if (last instanceof IsEmpty) {
                    StampedValue<Boolean> isEmpty = ((IsEmpty<?>) last).get();
                    ListenableFuture<?> future;
                    if (isEmpty.get().booleanValue()) {
                        future = UsePrecedingXomega.create(version, useXomega);
                    } else {
                        future = Futures.immediateCancelledFuture();
                    }
                    return Optional.of(future);
                } else if (last instanceof UsePrecedingXomega) {
                    ListenableFuture<Long> future;
                    if (((Boolean) last.get()).booleanValue()) {
                        VersionedId preceding;
                        materializer.cache().lock().readLock().lock();
                        try {
                            StorageSchema.Safari.Volumes.Volume.Log.Version node = (StorageSchema.Safari.Volumes.Volume.Log.Version) materializer.cache().cache().get(path);
                            preceding = node.log().versions().lowerEntry(node.parent().name()).getValue().id();
                        } finally {
                            materializer.cache().lock().readLock().unlock();
                        }
                        future = ComputeXalpha.forXomega(preceding, getXomega);
                    } else {
                        future = ComputeXalpha.forValue(Long.valueOf(((IsEmpty<?>) previous.next()).get().stamp()));
                    }
                    return Optional.of(future);
                } else if (last instanceof ComputeXalpha) {
                    Long xalpha = (Long) last.get();
                    return Optional.of(CreateXalpha.create(xalpha, path.join(StorageSchema.Safari.Volumes.Volume.Log.Version.Xalpha.LABEL), materializer));
                }
                return Optional.absent();
            }
            
        }
    }
    
    protected static final class IsLatest extends SimpleToStringListenableFuture<Boolean> {

        public static IsLatest create(VersionedId version,
                Function<Identifier, FutureTransition<UnsignedLong>> latest) {
            return new IsLatest(version, CompareToLatest.create(version, latest).call());
        }
        
        private final VersionedId version;
        
        protected IsLatest(
                VersionedId version,
                ListenableFuture<Boolean> delegate) {
            super(delegate);
            this.version = version;
        }

        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
            return super.toStringHelper(helper.addValue(version));
        }
        
        protected static final class CompareToLatest extends AbstractPair<VersionedId, Function<Identifier, FutureTransition<UnsignedLong>>> implements AsyncFunction<UnsignedLong, Boolean>, Callable<ListenableFuture<Boolean>> {

            public static CompareToLatest create(VersionedId version,
                    Function<Identifier, FutureTransition<UnsignedLong>> latest) {
                return new CompareToLatest(version, latest);
            }
            
            protected CompareToLatest(
                    VersionedId first,
                    Function<Identifier, FutureTransition<UnsignedLong>> second) {
                super(first, second);
            }
            
            @Override
            public ListenableFuture<Boolean> call() {
                FutureTransition<UnsignedLong> next = second.apply(first.getValue());
                if (next.getCurrent().isPresent()) {
                    return apply(next.getCurrent().get());
                } else {
                    return Futures.transform(next.getNext(), this);
                }
            }

            @Override
            public ListenableFuture<Boolean> apply(UnsignedLong input) {
                int cmp = input.compareTo(first.getVersion());
                if (cmp == 0) {
                    return Futures.immediateFuture(Boolean.TRUE);
                } else if (cmp > 0) {
                    return Futures.immediateFuture(Boolean.FALSE);
                } else {
                    FutureTransition<UnsignedLong> next = second.apply(first.getValue());
                    if (next.getCurrent().isPresent() && !next.getCurrent().get().equals(input)) {
                        return apply(next.getCurrent().get());
                    }
                    return Futures.transform(next.getNext(), this);
                }
            }
            
            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this).addValue(first).toString();
            }
        }
    }
    
    protected static final class IsEmpty<O extends Operation.ProtocolResponse<?>> extends ToStringListenableFuture<StampedValue<Boolean>>implements FutureCallback<Optional<StampedValue<List<String>>>>, Callable<Watchers.Query<O, Optional<StampedValue<List<String>>>>> {

        public static <O extends Operation.ProtocolResponse<?>> IsEmpty<O> create(
                ZNodePath path,
                ClientExecutor<? super Records.Request, O, ?> client) {
            GetChildrenQuery<O> query = GetChildrenQuery.create(client);
            return new IsEmpty<O>(path, query, SettableFuturePromise.<StampedValue<Boolean>>create());
        }

        private final ZNodePath path;
        private final GetChildrenQuery<O> query;
        private final Promise<StampedValue<Boolean>> promise;
        
        protected IsEmpty(
                ZNodePath path,
                GetChildrenQuery<O> query,
                Promise<StampedValue<Boolean>> promise) {
            this.path = path;
            this.query = query;
            this.promise = promise;
        }
        
        @Override
        public Watchers.Query<O, Optional<StampedValue<List<String>>>> call() {
            return query.apply(Pair.create(path, this));
        }

        @Override
        public void onSuccess(Optional<StampedValue<List<String>>> result) {
            if (result.isPresent()) {
                Boolean value = Boolean.TRUE;
                for (String child: result.get().get()) {
                    if (child.equals(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.LABEL.toString())) {
                        return;
                    } else {
                        value = Boolean.FALSE;
                    }
                }
                delegate().set(StampedValue.valueOf(result.get().stamp(), value));
            } else {
                delegate().cancel(false);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            delegate().setException(t);
        }
        
        @Override
        protected Promise<StampedValue<Boolean>> delegate() {
            return promise;
        }
    }
    
    protected static final class UsePrecedingXomega extends SimpleToStringListenableFuture<Boolean> {

        public static UsePrecedingXomega create(
                VersionedId version, 
                AsyncFunction<VersionedId, Boolean> useXomega) {
            ListenableFuture<Boolean> future;
            try {
                future = useXomega.apply(version);
            } catch (Exception e) {
                future = Futures.immediateFailedFuture(e);
            }
            return new UsePrecedingXomega(future);
        }
        
        protected UsePrecedingXomega(ListenableFuture<Boolean> delegate) {
            super(delegate);
        }
    }
    
    protected static final class ComputeXalpha extends SimpleToStringListenableFuture<Long> {

        public static ComputeXalpha forXomega(VersionedId version, AsyncFunction<VersionedId, Long> getXomega) {
            ListenableFuture<Long> xomega;
            try {
                xomega = getXomega.apply(version);
            } catch (Exception e) {
                xomega = Futures.immediateFailedFuture(e);
            }
            return new ComputeXalpha(Futures.transform(xomega, new LongPlusOne()));
        }
        
        public static ComputeXalpha forValue(Long value) {
            return new ComputeXalpha(Futures.immediateFuture(value));
        }
        
        protected ComputeXalpha(ListenableFuture<Long> delegate) {
            super(delegate);
        }
        
        protected static final class LongPlusOne implements Function<Long, Long> {

            protected LongPlusOne() {}
            
            @Override
            public Long apply(Long input) {
                return Long.valueOf(input.longValue() + 1L);
            }
        }
    }
    
    protected static final class CreateXalpha extends SimpleToStringListenableFuture<Long> {

        public static <O extends Operation.ProtocolResponse<?>> CreateXalpha create(Long xalpha, ZNodePath path, Materializer<StorageZNode<?>, O> materializer) {
            return new CreateXalpha(Callback.create(xalpha, path, materializer));
        }
        
        protected CreateXalpha(ListenableFuture<Long> future) {
            super(future);
        }
        
        private static final class Callback<O extends Operation.ProtocolResponse<?>> extends SimpleToStringListenableFuture<O> implements Callable<Optional<Long>> {
            
            protected static <O extends Operation.ProtocolResponse<?>> CallablePromiseTask<Callback<O>, Long> create(Long xalpha, ZNodePath path, Materializer<StorageZNode<?>, O> materializer) {
                ListenableFuture<O> future = materializer.create(path, xalpha).call();
                CallablePromiseTask<Callback<O>, Long> delegate = CallablePromiseTask.create(new Callback<O>(xalpha, future), SettableFuturePromise.<Long>create());
                future.addListener(delegate, MoreExecutors.directExecutor());
                return delegate;
            }
            
            private final Long value;
            
            private Callback(Long value, ListenableFuture<O> future) {
                super(future);
                this.value = value;
            }
            
            @Override
            public Optional<Long> call() throws Exception {
                if (isDone()) {
                    // at this point, any error indicates a leadership problem
                    Operations.unlessError(get().record());
                    return Optional.of(value);
                }
                return Optional.absent();
            }
        }
    }
}
