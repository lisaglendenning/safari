package edu.uw.zookeeper.safari.storage.snapshot;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Callback on snapshot commit.
 */
public final class SnapshotCommittedCallback<T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot>> extends Watchers.SimpleForwardingCallback<StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit,T> {

    public static Watchers.CacheNodeCreatedListener<StorageZNode<?>> listen(
            FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot> callback,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            WatchListeners cacheEvents,
            Service service,
            Logger logger) {
        return Watchers.CacheNodeCreatedListener.listen(
                cache, 
                service, 
                cacheEvents,
                Watchers.FutureCallbackListener.create(
                        Watchers.EventToPathCallback.create(
                                Watchers.PathToNodeCallback.create(
                                        create(callback), 
                                        cache.cache())), 
                        WatchMatcher.exact(
                                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH, 
                                Watcher.Event.EventType.NodeDataChanged), 
                        logger),
                logger);
    }

    public static <T extends FutureCallback<? super StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot>> SnapshotCommittedCallback<T> create(
            T delegate) {
        return new SnapshotCommittedCallback<T>(delegate);
    }
    
    protected SnapshotCommittedCallback(
            T delegate) {
        super(delegate);
    }
    
    @Override
    public void onSuccess(StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit result) {
        if ((result != null) && (result.data().get() != null) && result.data().get().booleanValue()) {
            delegate().onSuccess(result.snapshot());
        }
    }
}