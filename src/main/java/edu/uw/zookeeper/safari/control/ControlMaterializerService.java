package edu.uw.zookeeper.safari.control;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.WatchListeners;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.data.PrefixCreator;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.safari.peer.protocol.JacksonSerializer;

@DependsOn(ControlConnectionsService.class)
public class ControlMaterializerService extends ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Provides @Singleton
        public ControlMaterializerService getControlMaterializerService(
                Injector injector,
                ObjectMapper mapper,
                ControlConnectionsService<?> connections) {
            return ControlMaterializerService.newInstance(
                    injector, JacksonSerializer.create(mapper), connections);
        }
        
        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(
                    JacksonModule.create(),
                    ControlConnectionsService.module());
        }
    }
    
    public static ControlMaterializerService newInstance(
            Injector injector,
            Serializers.ByteCodec<Object> serializer, 
            ControlConnectionsService<?> connections) {
        return new ControlMaterializerService(
                injector, 
                serializer, 
                connections, 
                new StrongConcurrentSet<SessionListener>(), 
                SameThreadExecutor.getInstance());
    }

    protected final Injector injector;
    protected final Materializer<ControlZNode<?>, Message.ServerResponse<?>> materializer;
    protected final WatchListeners notifications;
    protected final CacheEventWatchListeners cacheEvents;

    protected ControlMaterializerService(
            Injector injector,
            Serializers.ByteCodec<Object> codec, 
            ControlConnectionsService<?> connections,
            IConcurrentSet<SessionListener> listeners,
            Executor executor) {
        super(connections.factory(), listeners, executor);
        this.injector = injector;
        this.materializer = Materializer.<ControlZNode<?>, Message.ServerResponse<?>>fromHierarchy(
                        ControlSchema.class,
                        codec,
                        this);
        this.notifications = WatchListeners.newInstance(materializer.schema().get());
        this.cacheEvents = new CacheEventWatchListeners(WatchListeners.newInstance(materializer.schema().get()));
    }
    
    public Materializer<ControlZNode<?>, Message.ServerResponse<?>> materializer() {
        return materializer;
    }

    public WatchListeners notifications() {
        return notifications;
    }

    public WatchListeners cacheEvents() {
        return cacheEvents.get();
    }
    
    @Override
    protected void startUp() throws Exception {
        materializer.cache().events().subscribe(cacheEvents);
        materializer.cache().subscribe(notifications);
        
        DependentService.addOnStart(injector, this);
        
        super.startUp();

        Futures.successfulAsList(PrefixCreator.forMaterializer(materializer()).call()).get();
    }

    @Override
    protected void shutDown() throws Exception {
        materializer.cache().unsubscribe(notifications);
        materializer.cache().events().unsubscribe(cacheEvents);
        
        super.shutDown();
    }
    
    public static class CacheEventWatchListeners implements Supplier<WatchListeners>, ZNodeCache.CacheListener {

        protected final WatchListeners watchers;
        
        public CacheEventWatchListeners(WatchListeners watchers) {
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
    }
}
