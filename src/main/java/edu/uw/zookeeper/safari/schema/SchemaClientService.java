package edu.uw.zookeeper.safari.schema;

import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.data.ZNodeCache.CacheEvents;
import edu.uw.zookeeper.protocol.Operation;

public class SchemaClientService<E extends Materializer.MaterializedNode<E,?>, O extends Operation.ProtocolResponse<?>> extends ServiceListenersService {

    public static <E extends Materializer.MaterializedNode<E,?>, O extends Operation.ProtocolResponse<?>> SchemaClientService<E,O> create(
            Materializer<E,O> materializer,
            Iterable<? extends Service.Listener> listeners) {
        return new SchemaClientService<E,O>(materializer, listeners);
    }
    
    protected final Materializer<E,O> materializer;
    protected final WatchListeners notifications;
    protected final WatchListeners cacheEvents;

    protected SchemaClientService(
            Materializer<E,O> materializer,
            Iterable<? extends Service.Listener> listeners) {
        this(WatchListeners.newInstance(materializer.schema().get()), 
                WatchListeners.newInstance(materializer.schema().get()), 
                materializer, listeners);
    }
    
    protected SchemaClientService(
            WatchListeners cacheEvents,
            WatchListeners notifications,
            Materializer<E,O> materializer,
            Iterable<? extends Service.Listener> listeners) {
        super(ImmutableList.<Service.Listener>builder()
                .add(new CacheEventWatchListeners(
                        materializer.cache().events(), 
                        cacheEvents))
                .add(new NotificationListeners(
                        materializer.cache(),
                        notifications))
                .addAll(listeners)
                .add(new CreatePrefix(materializer))
                .build());
        this.materializer = materializer;
        this.notifications = notifications;
        this.cacheEvents = cacheEvents;
    }
    
    public Materializer<E,O> materializer() {
        return materializer;
    }

    public WatchListeners notifications() {
        return notifications;
    }

    public WatchListeners cacheEvents() {
        return cacheEvents;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(((Class<?>)materializer.schema().get().root().get().getDeclaration()).getSimpleName()).toString();
    }

    public static class NotificationListeners extends Service.Listener implements Supplier<WatchListeners> {

        protected final ZNodeCache<?,?,?> events;
        protected final WatchListeners watchers;
        
        public NotificationListeners(
                ZNodeCache<?,?,?> events,
                WatchListeners watchers) {
            this.events = events;
            this.watchers = watchers;
        }
        
        @Override
        public WatchListeners get() {
            return watchers;
        }
        
        @Override
        public void starting() {
            events.subscribe(watchers);
        }
        
        @Override
        public void stopping(State from) {
            events.unsubscribe(watchers);
        }
    }
    
    public static class CacheEventWatchListeners extends Service.Listener implements Supplier<WatchListeners>, ZNodeCache.CacheListener {

        protected final CacheEvents events;
        protected final WatchListeners watchers;
        
        public CacheEventWatchListeners(
                CacheEvents events,
                WatchListeners watchers) {
            this.events = events;
            this.watchers = watchers;
        }
        
        @Override
        public WatchListeners get() {
            return watchers;
        }

        @Override
        public void handleCacheEvent(Set<NodeWatchEvent> events) {
            for (NodeWatchEvent event: events) {
                watchers.handleWatchEvent(event);
            }
        }
        
        @Override
        public void starting() {
            events.subscribe(this);
        }
        
        @Override
        public void stopping(State from) {
            events.unsubscribe(this);
        }
    }
    
    public static class CreatePrefix extends Service.Listener {
        
        protected final Materializer<?,?> materializer;
        
        public CreatePrefix(Materializer<?,?> materializer) {
            this.materializer = materializer;
        }
        
        @Override
        public void running() {
            try {
                Futures.successfulAsList(PrefixCreator.forMaterializer(materializer).call()).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
