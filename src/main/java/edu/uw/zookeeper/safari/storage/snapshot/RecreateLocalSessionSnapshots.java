package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.Watchers.CacheNodeCreatedListener;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.FutureChain;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.ToStringListenableFuture;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;
import edu.uw.zookeeper.safari.storage.snapshot.SequentialEphemeralTrieBuilder.SequentialNode;


public final class RecreateLocalSessionSnapshots<O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> & Connection.Listener<? super Operation.Response>> extends CacheNodeCreatedListener<StorageZNode<?>> {

    public static <O extends Operation.ProtocolResponse<?>,T extends ClientExecutor<? super Records.Request, ? extends Operation.ProtocolResponse<?>,?> & Connection.Listener<? super Operation.Response>> RecreateLocalSessionSnapshots<O,T> listen(
            final Service service,
            final SchemaClientService<StorageZNode<?>,O> client,
            final Function<? super Long, ? extends T> executors) {
        RecreateLocalSessionSnapshots<O,T> instance = new RecreateLocalSessionSnapshots<O,T>(client, executors, service);
        instance.listen();
        return instance;
    }

    protected final SchemaClientService<StorageZNode<?>,O> client;
    protected final Function<? super Long, ? extends T> executors;
    protected final ConcurrentMap<ZNodePath, RecreateSnapshot> snapshots;
    
    protected RecreateLocalSessionSnapshots(
            SchemaClientService<StorageZNode<?>,O> client,
            Function<? super Long, ? extends T> executors,
            Service service) {
        super(client.materializer().cache(), 
                service, 
                client.cacheEvents(),
                WatchMatcher.exact(
                        StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH,
                        Watcher.Event.EventType.NodeDataChanged));
        this.client = client;
        this.executors = executors;
        this.snapshots = new MapMaker().makeMap();
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        super.handleWatchEvent(event);
        final StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit commit = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit)
                cache.cache().get(event.getPath());
        if ((commit != null) && (commit.data().get() != null) && commit.data().get().booleanValue() && !snapshots.containsKey(commit.snapshot().path())) {
            new RecreateSnapshot(commit.snapshot().path()).run();
        }
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        for (RecreateSnapshot snapshot: Iterables.consumingIterable(snapshots.values())) {
            snapshot.cancel(false);
        }
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        for (RecreateSnapshot snapshot: Iterables.consumingIterable(snapshots.values())) {
            snapshot.setException(failure);
        }
        Services.stop(service);
    }
    
    protected final class RecreateSnapshot extends ForwardingPromise<ZNodePath> implements Runnable, ChainedFutures.ChainedProcessor<ListenableFuture<?>, FutureChain.FutureListChain<ListenableFuture<?>>> {
        
        private final ZNodePath snapshot;
        private final ChainedFutures.ChainedFuturesTask<ZNodePath> task;
        
        protected RecreateSnapshot(
                ZNodePath snapshot) {
            this.snapshot = snapshot;
            this.task = ChainedFutures.task(
                    ChainedFutures.<ZNodePath>castLast(
                            ChainedFutures.arrayList(this, 4)));
        }
  
        @Override
        public void run() {
            if (isDone()) {
                try {
                    get();
                } catch (Exception e) {
                    failed(state(), e);
                } finally {
                    snapshots.remove(snapshot, this);
                }
            } else {
                RecreateSnapshot existing = snapshots.putIfAbsent(snapshot, this);
                assert (existing == null);
                task.run();
                addListener(this, MoreExecutors.directExecutor());
            }
        }

        @Override
        public Optional<? extends ListenableFuture<?>> apply(
                FutureChain.FutureListChain<ListenableFuture<?>> input) throws Exception {
            switch (input.size()) {
            case 0:
            {
                ListenableFuture<?> future;
                try {
                    future = SequentialEphemeralTrieBuilder.create(
                            snapshot, 
                            client.materializer(), 
                            logger).call();
                } catch (Exception e) {
                    future = Futures.immediateFailedFuture(e);
                }
                return Optional.of(future);
            }
            case 1:
            {
                @SuppressWarnings("unchecked")
                Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>> sequentials = (Pair<SimpleLabelTrie<SequentialNode<AbsoluteZNodePath>>,? extends Set<AbsoluteZNodePath>>) input.getLast().get();
                return Optional.of(RecreateEphemerals.listen(
                        snapshot, 
                        sequentials.first(), 
                        sequentials.second(),
                        executors, 
                        client.materializer(), 
                        client.cacheEvents(), 
                        service, 
                        logger));
            }
            case 2:
            {
                input.getLast().get();
                final ZNodePath prefix;
                client.materializer().cache().lock().readLock().lock();
                try {
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) client.materializer().cache().cache().get(this.snapshot);
                    prefix = snapshot.version().log().volume().path().join(StorageSchema.Safari.Volumes.Volume.Root.LABEL).join((ZNodeName) snapshot.prefix().data().get());
                } finally {
                    client.materializer().cache().lock().readLock().unlock();
                }
                return Optional.of(RecreateWatches.listen(
                        snapshot,
                        new Function<ZNodeName, ZNodePath>() {
                            @Override
                            public ZNodePath apply(ZNodeName input) {
                                return prefix.join(input);
                            }
                        },
                        executors, 
                        client.materializer(), 
                        client.cacheEvents(), 
                        service, 
                        logger));
            }
            case 3:
            {
                return Optional.of(Futures.immediateFuture(snapshot));
            }
            default:
                break;
            }
            return Optional.absent();
        }
        
        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
            return helper.addValue(ToStringListenableFuture.toString(delegate()));
        }

        @Override
        protected Promise<ZNodePath> delegate() {
            return task;
        }
    }
}
