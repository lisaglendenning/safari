package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import edu.uw.zookeeper.client.LoggingWatchMatchListener;
import edu.uw.zookeeper.client.WatchMatchServiceListener;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

/**
 * Deletes snapshot state for expired sessions.
 * 
 * Assumes that session state and snapshot state are already watched.
 */
public final class ExpireSnapshotSessions extends WatchMatchServiceListener {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public ExpireSnapshotSessions getExpireSnapshotSessions(
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> client,
                final RegionRoleService service) {
            return ExpireSnapshotSessions.listen(
                    client.materializer(), 
                    client.cacheEvents(),
                    service);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(ExpireSnapshotSessions.class);
        }

        @Override
        protected void configure() {
        }
    }
    
    public static ExpireSnapshotSessions listen(
            Materializer<StorageZNode<?>,?> materializer,
            WatchListeners cacheEvents,
            Service service) {
        final Set<AbsoluteZNodePath> snapshots = Collections.synchronizedSet(
                Sets.<AbsoluteZNodePath>newHashSet());
        ExpireSnapshotSessions instance = new ExpireSnapshotSessions(snapshots, materializer, cacheEvents, service);
        Watchers.CacheNodeCreatedListener.create(
                materializer.cache(),
                service, 
                cacheEvents,
                instance.new SnapshotWatcher(snapshots, instance.logger()),
                instance.logger()).listen();
        instance.listen();
        return instance;
    }

    protected final Materializer<StorageZNode<?>,?> materializer;
    protected final Set<AbsoluteZNodePath> snapshots;
    
    protected ExpireSnapshotSessions(
            Set<AbsoluteZNodePath> snapshots,
            Materializer<StorageZNode<?>,?> materializer,
            WatchListeners cacheEvents,
            Service service) {
        super(service, 
                cacheEvents, 
                WatchMatcher.exact(
                        StorageSchema.Safari.Sessions.Session.PATH, 
                        Watcher.Event.EventType.NodeDeleted));
        this.materializer = materializer;
        this.snapshots = snapshots;
    }

    @Override
    public void handleWatchEvent(WatchEvent event) {
        super.handleWatchEvent(event);
        ImmutableList<AbsoluteZNodePath> expired = ImmutableList.of();
        final ZNodeLabel label = (ZNodeLabel) event.getPath().label();
        synchronized (snapshots) {
            for (AbsoluteZNodePath path: snapshots) {
                StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot snapshot = 
                        (StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot) materializer.cache().cache().get(path);
                // only delete committed snapshot state
                if ((snapshot != null) && (snapshot.commit() != null)) {
                    for (StorageZNode<?> node: snapshot.values()) {
                        StorageZNode<?> session = node.get(label);
                        if (session != null) {
                            expired = ImmutableList.<AbsoluteZNodePath>builder()
                            .addAll(expired)
                            .add((AbsoluteZNodePath) session.path()).build();
                        }
                    }
                }
            }
        }
        for (AbsoluteZNodePath path: expired) {
            delete(path);
        }
    }
    
    @Override
    public void terminated(Service.State from) {
        super.terminated(from);
        stop();
    }
    
    @Override
    public void failed(Service.State from, Throwable failure) {
        super.failed(from, failure);
        stop();
        Services.stop(service);
    }
    
    protected void stop() {
        snapshots.clear();
    }
    
    protected ListenableFuture<AbsoluteZNodePath> delete(AbsoluteZNodePath path) {
        // TODO handle errors
        return DeleteSubtree.deleteAll(path, materializer);
    }
    
    /**
     * Tracks committed snapshots and checks for expired sessions when snapshot state is committed.
     */
    protected final class SnapshotWatcher extends LoggingWatchMatchListener {

        private final Set<AbsoluteZNodePath> snapshots;
        private final PathToQuery<?,?> query;
        private final Watchers.MaybeErrorProcessor processor;
        
        @SuppressWarnings("unchecked")
        public SnapshotWatcher(
                Set<AbsoluteZNodePath> snapshots,
                Logger logger) {
            super(WatchMatcher.exact(
                    StorageSchema.Safari.Volumes.Volume.Log.Version.Snapshot.Commit.PATH, 
                    Watcher.Event.EventType.NodeCreated, Watcher.Event.EventType.NodeDeleted), 
                logger);
            this.snapshots = snapshots;
            this.query = PathToQuery.forRequests(materializer, 
                    Operations.Requests.sync(), 
                    Operations.Requests.exists());
            this.processor = Watchers.MaybeErrorProcessor.maybeNoNode();
        }
        
        public Set<AbsoluteZNodePath> getSnapshots() {
            return snapshots;
        }
        
        @Override
        public void handleWatchEvent(WatchEvent event) {
            super.handleWatchEvent(event);
            final AbsoluteZNodePath snapshot = (AbsoluteZNodePath) ((AbsoluteZNodePath) event.getPath()).parent();
            final LockableZNodeCache<StorageZNode<?>,?,?> cache = materializer.cache();
            switch (event.getEventType()) {
            case NodeCreated:
            {
                snapshots.add(snapshot);
                for (StorageZNode<?> node: cache.cache().get(snapshot).values()) {
                    for (StorageZNode<?> session: node.values()) {
                        ZNodePath path = StorageSchema.Safari.Sessions.Session.pathOf(((StorageZNode.SessionZNode<?>) session).name());
                        if (!cache.cache().containsKey(path)) {
                            Watchers.Query.call(
                                    processor, 
                                    new Callback((AbsoluteZNodePath) session.path()), 
                                    query.apply(path)).run();
                            
                        }
                    }
                }
                break;
            }
            case NodeDeleted:
                snapshots.remove(snapshot);
                break;
            default:
                break;
            }
        }
        
        protected final class Callback extends Watchers.FailWatchListener<Optional<Operation.Error>> {

            private final AbsoluteZNodePath path;
            
            protected Callback(AbsoluteZNodePath path) {
                super(ExpireSnapshotSessions.this);
                this.path = path;
            }
            
            @Override
            public void onSuccess(Optional<Operation.Error> result) {
                if (result.isPresent()) {
                    delete(path);
                }
            }
        }
    }
}
