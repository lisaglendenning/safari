package edu.uw.zookeeper.safari.storage;

import org.apache.zookeeper.Watcher;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.AbstractWatchListener;
import edu.uw.zookeeper.client.FixedQuery;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class SessionsWatcher extends AbstractWatchListener {

    public static SessionsWatcher listen(
            StorageClientService storage,
            Service service) {
        SessionsWatcher instance = new SessionsWatcher(storage, service);
        instance.listen();
        return instance;
    }
    
    protected final StorageClientService storage;
    protected final PathToQuery<?,Message.ServerResponse<?>> query;
    
    @SuppressWarnings("unchecked")
    protected SessionsWatcher(
            StorageClientService storage,
            Service service) {
        super(service, 
                storage.notifications(), 
                WatchMatcher.exact(
                        StorageSchema.Safari.Sessions.Session.PATH, 
                        Watcher.Event.EventType.NodeDataChanged));
        this.storage = storage;
        this.query = PathToQuery.forRequests(storage.materializer(), 
                Operations.Requests.sync(), 
                Operations.Requests.getData().setWatch(true));
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        Watchers.Query.call(query.apply(event.getPath()), this);
    }
    
    @Override
    public void starting() {
        super.starting();
        
        new SessionCreatedListener().listen();
    }
    
    protected class SessionCreatedListener extends Watchers.CacheNodeCreatedListener<StorageZNode<?>> {

        public SessionCreatedListener() {
            super(SessionsWatcher.this.getWatchMatcher().getPath(), 
                    storage.materializer().cache(), 
                    SessionsWatcher.this.service, 
                    storage.cacheEvents());
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            SessionsWatcher.this.handleWatchEvent(event);
        }
        
        @Override
        public void running() {
            final ZNodePath parent = ((AbsoluteZNodePath) getWatchMatcher().getPath()).parent();
            Watchers.RunnableWatcher.listen(
                    new Runnable() {
                        @SuppressWarnings("unchecked")
                        final FixedQuery<?> query = FixedQuery.forIterable(
                                storage.materializer(), 
                                PathToRequests.forRequests(
                                        Operations.Requests.sync(), 
                                        Operations.Requests.getChildren().setWatch(true))
                                        .apply(parent));
                        @Override
                        public void run() {
                            Watchers.Query.call(query, SessionsWatcher.this);
                        }
                    },
                    service, 
                    watch, 
                    WatchMatcher.exact(
                            parent, 
                            Watcher.Event.EventType.NodeChildrenChanged));
        }
    }
}
