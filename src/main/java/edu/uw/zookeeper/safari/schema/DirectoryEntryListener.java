package edu.uw.zookeeper.safari.schema;

import java.util.EnumSet;
import java.util.Iterator;

import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.Watcher;

import com.google.common.base.Objects;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.WatchMatchListener;
import edu.uw.zookeeper.data.WatchMatcher;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.proto.Records;

/**
 * Listens for directory entry events in the cache.
 */
public class DirectoryEntryListener<U extends SafariZNode<U,?>, T extends U> extends Watchers.CacheNodeCreatedListener<U> {

    public static <U extends SafariZNode<U,?>, T extends U> DirectoryEntryListener<U,T> create(
            Class<?> type,
            SchemaClientService<U,?> client,
            Service service,
            Logger logger) {
        return create(
                client.materializer().schema().apply(type),
                ImmutableList.<WatchMatchListener>of(),
                client.materializer().cache(),
                service,
                client.cacheEvents(),
                logger);
    }
    
    public static <U extends SafariZNode<U,?>, T extends U> DirectoryEntryListener<U,T> create(
            ValueNode<ZNodeSchema> schema,
            Iterable<? extends WatchMatchListener> listeners,
            LockableZNodeCache<U,Records.Request,?> cache,
            Service service,
            WatchListeners cacheEvents,
            Logger logger) {
        return new DirectoryEntryListener<U,T>(
                schema,
                listeners,
                cache,
                service,
                cacheEvents,
                logger);
    }

    public static Watchers.FutureCallbackListener<?> entryCreatedQuery(
            final PathToQuery<?,?> query,
            final DirectoryEntryListener<?,?> service) {
        return entryCreatedCallback(
                Watchers.EventToPathCallback.create(
                    Watchers.PathToQueryCallback.create(
                            query, 
                            Watchers.MaybeErrorProcessor.maybeNoNode(), 
                            Watchers.FailWatchListener.create(service))),
                service);
    }

    public static Watchers.FutureCallbackListener<?> entryCreatedCallback(
            final FutureCallback<? super WatchEvent> callback,
            final DirectoryEntryListener<?,?> service) {
        Watchers.FutureCallbackListener<?> listener = Watchers.FutureCallbackListener.create(
                callback, 
                WatchMatcher.exact(
                        service.schema().path(), 
                        Watcher.Event.EventType.NodeCreated), 
                service.logger());
        service.add(listener);
        return listener;
    }

    protected final ValueNode<ZNodeSchema> schema;

    protected DirectoryEntryListener(
            ValueNode<ZNodeSchema> schema,
            Iterable<? extends WatchMatchListener> listeners,
            LockableZNodeCache<U,Records.Request,?> cache,
            Service service,
            WatchListeners cacheEvents,
            Logger logger) {
        this(schema, listeners,
                cache,
                service, 
                cacheEvents,
                Watchers.DispatchingWatchMatchListener.create(
                        listeners, 
                        WatchMatcher.exact(
                                schema.path(), 
                                EnumSet.allOf(Watcher.Event.EventType.class)), 
                        logger),
                logger);
    }

    protected DirectoryEntryListener(
            ValueNode<ZNodeSchema> schema,
            Iterable<? extends WatchMatchListener> listeners,
            LockableZNodeCache<U,Records.Request,?> cache,
            Service service,
            WatchListeners cacheEvents,
            Watchers.DispatchingWatchMatchListener watcher,
            Logger logger) {
        super(cache,
                service, 
                cacheEvents,
                watcher,
                logger);
        this.schema = schema;
    }
    
    public ValueNode<ZNodeSchema> schema() {
        return schema;
    }
    
    public boolean add(WatchMatchListener listener) {
        boolean added = getWatcher().getListeners().add(listener);
        if (added && isRunning()) {
            cache.lock().readLock().lock();
            try {
                Iterator<? extends WatchEvent> events = replay();
                while (events.hasNext()) {
                    WatchEvent event = events.next();
                    Watchers.DispatchingWatchMatchListener.deliver(event, listener);
                }
            } finally {
                cache.lock().readLock().unlock();
            }
        }
        return added;
    }

    @Override
    public Watchers.DispatchingWatchMatchListener getWatcher() {
        return (Watchers.DispatchingWatchMatchListener) watcher;
    }
    
    @Override
    public void failed(Service.State from, Throwable failure){
        super.failed(from, failure);
        Services.stop(service); 
    }
    
    @Override
    protected Iterator<? extends WatchEvent> replay() {
        return ReplayDirectoryEntries.fromTrie(schema, cache.cache());
    }

    @Override
    protected Objects.ToStringHelper toString(Objects.ToStringHelper helper) {
        return super.toString(helper.addValue(((Class<?>) schema.get().getDeclaration()).getName()));
    }
    
    public static class ReplayDirectoryEntries<U extends SafariZNode<U,?>> extends AbstractIterator<NodeWatchEvent> {

        public static <U extends SafariZNode<U,?>> ReplayDirectoryEntries<U> fromTrie(
                ValueNode<ZNodeSchema> schema, NameTrie<U> trie) {
            final U directory = trie.get(schema.parent().get().path());
            return new ReplayDirectoryEntries<U>(schema, directory.values().iterator());
        }
        
        protected final ValueNode<ZNodeSchema> schema;
        protected final Iterator<U> entries;
        
        protected ReplayDirectoryEntries(
                ValueNode<ZNodeSchema> schema,
                Iterator<U> entries) {
            this.schema = schema;
            this.entries = entries;
        }
        
        @Override
        protected NodeWatchEvent computeNext() {
            while (entries.hasNext()) {
                U entry = entries.next();
                if (entry.schema() == schema) {
                    return NodeWatchEvent.nodeCreated(entry.path());
                }
            }
            return endOfData();
        }
    }
}
