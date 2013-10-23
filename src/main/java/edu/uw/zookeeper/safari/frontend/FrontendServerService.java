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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.Server;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.DependentModule;
import edu.uw.zookeeper.safari.common.DependentService;
import edu.uw.zookeeper.safari.common.DependsOn;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;
import edu.uw.zookeeper.server.ServerConnectionFactoryBuilder;

@DependsOn({FrontendServerExecutor.class})
public class FrontendServerService<C extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> extends ServerConnectionsHandler<C> {

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
            ServerConnectionFactory<? extends ProtocolCodecConnection<Server, ServerProtocolCodec, Connection<Server>>> connections = 
                    getServerConnectionFactory(runtime, configuration, serverModule);
            FrontendServerService<?> instance = FrontendServerService.newInstance(
                    server, executor, configuration.getTimeOut(), injector);
            connections.subscribe(instance);
            return instance;
        }
        
        protected ServerConnectionFactory<? extends ProtocolCodecConnection<Server, ServerProtocolCodec, Connection<Server>>> getServerConnectionFactory(
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
                    RegionConnectionsService.module(),
                    FrontendConfiguration.module(),
                    FrontendServerExecutor.module());
        }
    }
    
    public static class AllVolumesAssigned implements Processor<Object, Optional<Boolean>> {
        
        public static ZNodeLabel.Path root() {
            return ROOT;
        }
        
        public static ListenableFuture<Boolean> call(Materializer<?> materializer) {
            return Control.FetchUntil.newInstance(
                    AllVolumesAssigned.root(), 
                    new AllVolumesAssigned(materializer), 
                    materializer);
        }
        
        protected static final ZNodeLabel.Path ROOT = Control.path(ControlSchema.Volumes.class);
        
        protected final Materializer<?> materializer;
        
        public AllVolumesAssigned(Materializer<?> materializer) {
            this.materializer = materializer;
        }

        @Override
        public Optional<Boolean> apply(Object input) throws Exception {
            Materializer.MaterializedNode root = materializer.get(ROOT);
            if (root != null) {
                for (Materializer.MaterializedNode e: root.values()) {
                    if (! e.containsKey(ControlSchema.Volumes.Entity.Region.LABEL)) {
                        return Optional.absent();
                    }
                }
                return Optional.of(Boolean.valueOf(true));
            }
            return Optional.absent();
        }
    }

    public static <C extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> FrontendServerService<C> newInstance(
            ServerExecutor<?> server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut,
            Injector injector) {
        ConcurrentMap<C, ServerConnectionsHandler<C>.ConnectionHandler<?,?>> handlers = new MapMaker().weakKeys().weakValues().makeMap();
        FrontendServerService<C> instance = new FrontendServerService<C>(
                server,
                scheduler,
                timeOut,
                handlers,
                injector);
        new Advertiser(instance, injector);
        return instance;
    }

    protected final Logger logger;
    protected final Injector injector;
    
    protected FrontendServerService(
            ServerExecutor<?> server, 
            ScheduledExecutorService scheduler, 
            TimeValue timeOut,
            ConcurrentMap<C, ConnectionHandler<?,?>> handlers,
            Injector injector) {
        super(server, scheduler, timeOut, handlers);
        this.logger = LogManager.getLogger(getClass());
        this.injector = injector;
    }
    
    protected Injector injector() {
        return injector;
    }

    @Override
    protected void startUp() throws Exception {
        DependentService.addOnStart(injector, this);

        // global barrier - wait for all volumes to be assigned
        AllVolumesAssigned.call( 
                injector().getInstance(ControlMaterializerService.class).materializer()).get();

        super.startUp();
    }

    public static class Advertiser extends Service.Listener {

        protected final Logger logger;
        protected final Injector injector;
        
        @Inject
        public Advertiser(
                FrontendServerService<?> server,
                Injector injector) {
            this(server, injector, MoreExecutors.sameThreadExecutor());
        }

        public Advertiser(
                FrontendServerService<?> server,
                Injector injector,
                Executor executor) {
            this.logger = LogManager.getLogger(getClass());
            this.injector = injector;
            server.addListener(this, executor);
            if (server.isRunning()) {
                running();
            }
        }
        
        @Override
        public void running() {
            Materializer<?> materializer = injector.getInstance(ControlMaterializerService.class).materializer();
            Identifier peerId = injector.getInstance(PeerConfiguration.class).getView().id();
            ServerInetAddressView address = injector.getInstance(FrontendConfiguration.class).getAddress();
            try {
                FrontendConfiguration.advertise(peerId, address, materializer);
            } catch (Exception e) {
                logger.warn("", e);
                injector.getInstance(FrontendServerService.class).stopAsync();
            }
        }
    }
}
