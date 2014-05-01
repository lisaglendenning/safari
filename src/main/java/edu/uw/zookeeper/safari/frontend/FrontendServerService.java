package edu.uw.zookeeper.safari.frontend;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.server.ServerConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.ServerProtocolConnection;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.FetchUntil;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;

@DependsOn({FrontendServerExecutor.class})
public class FrontendServerService<C extends ServerProtocolConnection<?,?>> extends ServerConnectionsHandler<C> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends DependentModule {

        public Module() {}
        
        @Override
        protected void configure() {
            super.configure();
            bind(FrontendServerService.class).to(new TypeLiteral<FrontendServerService<?>>() {});
        }

        @Provides @Singleton
        public FrontendServerService<?> getFrontendServerService(
                ServerExecutor<FrontendSessionExecutor> server, 
                ScheduledExecutorService scheduler,
                RuntimeModule runtime,
                FrontendConfiguration configuration, 
                ScheduledExecutorService executor,
                Injector injector,
                NetServerModule serverModule) throws Exception {
            ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> connections = 
                    getServerConnectionFactory(runtime, configuration, serverModule);
            FrontendServerService<?> instance = FrontendServerService.newInstance(
                    connections, server, executor, configuration.getTimeOut(), injector);
            return instance;
        }
        
        protected ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> getServerConnectionFactory(
                RuntimeModule runtime,
                FrontendConfiguration configuration,
                NetServerModule serverModule) {
            return ServerConnectionFactoryBuilder.defaults()
                    .setAddress(configuration.getAddress())
                    .setServerModule(serverModule)
                    .setRuntimeModule(runtime)
                    .build();
        }

        @Override
        protected List<com.google.inject.Module> getDependentModules() {
            return ImmutableList.<com.google.inject.Module>of(
                    RegionsConnectionsService.module(),
                    FrontendConfiguration.module(),
                    FrontendServerExecutor.module());
        }
    }
    
    public static class AllVolumesAvailable implements Processor<Object, Optional<Boolean>> {
        
        public static ListenableFuture<Boolean> call(Materializer<ControlZNode<?>,?> materializer) {
            return FetchUntil.newInstance(
                    ControlSchema.Safari.Volumes.PATH, 
                    new AllVolumesAvailable(materializer), 
                    materializer);
        }
        
        protected final Materializer<ControlZNode<?>,?> materializer;
        
        public AllVolumesAvailable(Materializer<ControlZNode<?>,?> materializer) {
            this.materializer = materializer;
        }

        @Override
        public Optional<Boolean> apply(Object input) throws Exception {
            materializer.cache().lock().readLock().lock();
            try {
                ControlSchema.Safari.Volumes root = ControlSchema.Safari.Volumes.get(materializer.cache().cache());
                if (root != null) {
                    for (ControlZNode<?> e: root.values()) {
                        ControlSchema.Safari.Volumes.Volume.Log log = ((ControlSchema.Safari.Volumes.Volume) e).getLog();
                        // for now we'll use the existence of a log version
                        // as a proxy for a volume being active
                        if ((log == null) || log.isEmpty()) {
                            return Optional.absent();
                        }
                    }
                    return Optional.of(Boolean.valueOf(true));
                }
                return Optional.absent();
            } finally {
                materializer.cache().lock().readLock().unlock();
            }
        }
    }

    public static <C extends ServerProtocolConnection<?,?>> FrontendServerService<C> newInstance(
            ServerConnectionFactory<C> connections,
            ServerExecutor<?> server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut,
            Injector injector) {
        ConcurrentMap<C, ServerConnectionsHandler<C>.ConnectionHandler<?>> handlers = new MapMaker().weakKeys().weakValues().makeMap();
        FrontendServerService<C> instance = new FrontendServerService<C>(
                connections,
                server,
                scheduler,
                timeOut,
                handlers,
                injector);
        instance.new Advertiser(MoreExecutors.sameThreadExecutor());
        return instance;
    }

    protected final Logger logger;
    protected final Injector injector;
    protected final ServerConnectionFactory<C> connections;
    
    protected FrontendServerService(
            ServerConnectionFactory<C> connections,
            ServerExecutor<?> server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut,
            ConcurrentMap<C, ConnectionHandler<?>> handlers,
            Injector injector) {
        super(server, scheduler, timeOut, handlers);
        this.logger = LogManager.getLogger(getClass());
        this.injector = injector;
        this.connections = connections;
    }
    
    protected Injector injector() {
        return injector;
    }

    @Override
    protected void startUp() throws Exception {
        DependentService.addOnStart(injector, this);

        // global barrier - wait for all volumes to be assigned
        AllVolumesAvailable.call( 
                injector().getInstance(ControlMaterializerService.class).materializer()).get();

        connections.subscribe(this);
        connections.startAsync().awaitRunning();
        injector().getInstance(ServiceMonitor.class).add(connections);

        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        connections.unsubscribe(this);
        super.shutDown();
    }
    
    public class Advertiser extends Service.Listener {

        public Advertiser(
                Executor executor) {
            addListener(this, executor);
            if (isRunning()) {
                running();
            }
        }
        
        @Override
        public void running() {
            Materializer<ControlZNode<?>,?> materializer = injector.getInstance(ControlMaterializerService.class).materializer();
            Identifier peerId = injector.getInstance(PeerConfiguration.class).getView().id();
            ServerInetAddressView address = injector.getInstance(FrontendConfiguration.class).getAddress();
            try {
                FrontendConfiguration.advertise(peerId, address, materializer).get();
            } catch (Exception e) {
                logger.warn("", e);
                stopAsync();
            }
        }
    }
}
