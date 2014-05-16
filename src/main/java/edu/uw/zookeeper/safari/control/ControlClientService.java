package edu.uw.zookeeper.safari.control;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.data.ZNodeCache.CacheEvents;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.safari.data.PrefixCreator;

public class ControlClientService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        public Module() {}
        
        @Provides @Control @Singleton
        public ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> newControlClient(
                @Control EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> connections,
                ServiceMonitor monitor,
                ScheduledExecutorService scheduler) {
            ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> instance = 
                    ConnectionClientExecutorService.newInstance(connections, scheduler);
            monitor.add(instance);
            return instance;
        }

        @Provides @Control @Singleton
        public Materializer<ControlZNode<?>, Message.ServerResponse<?>> newControlClientMaterializer(
                ControlClientService service) {
            return service.materializer();
        }
        
        @Provides @Singleton
        public ControlClientService newControlClientService(
                final @Control ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
                final @Control ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> client,
                Serializers.ByteCodec<Object> serializer,
                ServiceMonitor monitor) {
            ControlClientService instance = ControlClientService.forClient(
                    client,
                    serializer,
                    ImmutableList.<Service.Listener>of(
                            new Service.Listener() {
                                @Override
                                public void starting() {
                                    Services.startAndWait(connections);
                                }
                            }));
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {
            bind(Key.get(new TypeLiteral<Materializer<ControlZNode<?>, ?>>(){}, Control.class)).to(Key.get(new TypeLiteral<Materializer<ControlZNode<?>, Message.ServerResponse<?>>>(){}, Control.class));
        }
    }

    public static ControlClientService forClient(
            final ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> client,
            Serializers.ByteCodec<Object> serializer,
            Iterable<? extends Service.Listener> listeners) {
        return create(
                Materializer.<ControlZNode<?>, Message.ServerResponse<?>>fromHierarchy(
                    ControlSchema.class,
                    serializer,
                    client),
                ImmutableList.<Service.Listener>builder()
                    .addAll(listeners)
                    .add(new Service.Listener() {
                                @Override
                                public void starting() {
                                    Services.startAndWait(client);
                                }
                            }).build());
    }
            
    public static ControlClientService create(
            Materializer<ControlZNode<?>, Message.ServerResponse<?>> materializer,
            Iterable<? extends Service.Listener> listeners) {
        final NotificationListeners notifications = new NotificationListeners(
                materializer.cache(),
                WatchListeners.newInstance(materializer.schema().get()));
        final CacheEventWatchListeners cacheEvents = new CacheEventWatchListeners(
                materializer.cache().events(),
                WatchListeners.newInstance(materializer.schema().get()));
        ControlClientService instance = new ControlClientService(
                materializer,
                notifications.get(),
                cacheEvents.get(),
                ImmutableList.<Service.Listener>builder()
                    .addAll(listeners)
                    .add(notifications)
                    .add(cacheEvents)
                    .add(new CreatePrefix(materializer)).build());
        return instance;
    }

    protected final Materializer<ControlZNode<?>, Message.ServerResponse<?>> materializer;
    protected final WatchListeners notifications;
    protected final WatchListeners cacheEvents;

    protected ControlClientService(
            Materializer<ControlZNode<?>, Message.ServerResponse<?>> materializer,
            WatchListeners notifications,
            WatchListeners cacheEvents,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.materializer = materializer;
        this.notifications = notifications;
        this.cacheEvents = cacheEvents;
    }
    
    public Materializer<ControlZNode<?>, Message.ServerResponse<?>> materializer() {
        return materializer;
    }

    public WatchListeners notifications() {
        return notifications;
    }

    public WatchListeners cacheEvents() {
        return cacheEvents;
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
        
        protected final Materializer<ControlZNode<?>,?> materializer;
        
        public CreatePrefix(Materializer<ControlZNode<?>,?> materializer) {
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
