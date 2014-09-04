package edu.uw.zookeeper.safari.storage;

import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class SessionsWatcher extends Watchers.CacheNodeCreatedListener<StorageZNode<?>> {
    
    @SuppressWarnings("unchecked")
    public static SessionsWatcher listen(
            SchemaClientService<StorageZNode<?>,?> client,
            Service service) {
        SessionsWatcher instance = new SessionsWatcher(
                Watchers.EventToPathCallback.create(
                        Watchers.PathToQueryCallback.create(
                                PathToQuery.forRequests(
                                        client.materializer(), 
                                        Operations.Requests.sync(), 
                                        Operations.Requests.getData().setWatch(true)),
                                Watchers.MaybeErrorProcessor.maybeNoNode(),
                                Watchers.StopServiceOnFailure.create(service))),
                client.materializer().cache(),
                service, 
                client.cacheEvents());
        Watchers.FutureCallbackServiceListener.listen(
                Watchers.EventToPathCallback.create(
                        Watchers.PathToQueryCallback.create(
                                PathToQuery.forRequests(
                                        client.materializer(), 
                                        Operations.Requests.sync(), 
                                        Operations.Requests.getData().setWatch(true)),
                                Watchers.MaybeErrorProcessor.maybeNoNode(),
                                Watchers.StopServiceOnFailure.create(service))), 
                service, 
                client.notifications(), 
                WatchMatcher.exact(
                        StorageSchema.Safari.Sessions.Session.PATH, 
                        Watcher.Event.EventType.NodeDataChanged), 
                LogManager.getLogger(instance));
        instance.listen();
        Watchers.watchChildren(
                StorageSchema.Safari.Sessions.PATH, 
                client.materializer(), 
                service, 
                client.notifications());
        return instance;
    }
    
    protected final FutureCallback<? super WatchEvent> callback;
    
    protected SessionsWatcher(
            FutureCallback<? super WatchEvent> callback,
            LockableZNodeCache<StorageZNode<?>,?,?> cache,
            Service service,
            WatchListeners watch) {
        super(StorageSchema.Safari.Sessions.Session.PATH, 
                cache,
                service, 
                watch);
        this.callback = callback;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        super.handleWatchEvent(event);
        callback.onSuccess(event);
    }
    
    @Override
    protected Iterator<? extends WatchEvent> replay() {
        final StorageSchema.Safari.Sessions directory = StorageSchema.Safari.Sessions.fromTrie(cache.cache());
        return Iterators.unmodifiableIterator(
                Iterators.transform(
                    directory.values().iterator(),
                    new Function<StorageZNode<?>, NodeWatchEvent>() {
                        @Override
                        public NodeWatchEvent apply(StorageZNode<?> input) {
                            return NodeWatchEvent.nodeCreated(input.path());
                        }
                    }));
    }
}
