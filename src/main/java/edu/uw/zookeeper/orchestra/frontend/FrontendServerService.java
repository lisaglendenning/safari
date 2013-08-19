package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.DependentModule;
import edu.uw.zookeeper.orchestra.common.Dependencies;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutor;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.server.ServerApplicationModule;

@DependsOn({FrontendServerExecutor.class})
public class FrontendServerService<T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> extends ServerConnectionExecutorsService<T> {

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
                FrontendConfiguration configuration, 
                ServerTaskExecutor serverExecutor,
                ServiceLocator locator,
                NetServerModule servers,
                DependentServiceMonitor monitor) throws Exception {
            ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections = 
                    servers.getServerConnectionFactory(
                            ServerApplicationModule.codecFactory(),
                            ServerApplicationModule.connectionFactory())
                    .get(configuration.getAddress().get());
            return monitor.listen(
                    FrontendServerService.newInstance(connections, serverExecutor, locator));
        }

        @Override
        protected com.google.inject.Module[] getModules() {
            com.google.inject.Module[] modules = { 
                    EnsembleConnectionsService.module(),
                    FrontendConfiguration.module(),
                    FrontendServerExecutor.module() };
            return modules;
        }
    }
    
    public static <T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> FrontendServerService<T> newInstance(
            ServerConnectionFactory<T> connections,
            ServerTaskExecutor server,
            ServiceLocator locator) {
        FrontendServerService<T> instance = new FrontendServerService<T>(
                connections,             
                ServerConnectionExecutor.<T>factory(
                        server.getAnonymousExecutor(), 
                        server.getConnectExecutor(), 
                        server.getSessionExecutor()), 
                locator);
        instance.new Advertiser(MoreExecutors.sameThreadExecutor());
        return instance;
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
                    if (! e.containsKey(ControlSchema.Volumes.Entity.Ensemble.LABEL)) {
                        return Optional.absent();
                    }
                }
                return Optional.of(Boolean.valueOf(true));
            }
            return Optional.absent();
        }
    }

    protected final ServiceLocator locator;
    
    protected FrontendServerService(
            ServerConnectionFactory<T> connections,
            ParameterizedFactory<T, ServerConnectionExecutor<T>> factory,
            ServiceLocator locator) {
        super(connections, factory);
        this.locator = locator;
    }
    
    protected ServiceLocator locator() {
        return locator;
    }

    @Override
    protected void startUp() throws Exception {
        Dependencies.startDependenciesAndWait(this, locator());
        
        // global barrier - wait for all volumes to be assigned
        AllVolumesAssigned.call( 
                locator().getInstance(ControlMaterializerService.class).materializer()).get();

        super.startUp();
    }

    public class Advertiser implements Service.Listener {

        public Advertiser(Executor executor) {
            addListener(this, executor);
        }
        
        @Override
        public void starting() {
        }

        @Override
        public void running() {
            Materializer<?> materializer = locator().getInstance(ControlMaterializerService.class).materializer();
            Identifier peerId = locator().getInstance(PeerConfiguration.class).getView().id();
            ServerInetAddressView address = locator().getInstance(FrontendConfiguration.class).getAddress();
            try {
                FrontendConfiguration.advertise(peerId, address, materializer);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void stopping(State from) {
        }

        @Override
        public void terminated(State from) {
        }

        @Override
        public void failed(State from, Throwable failure) {
        }
    }
}
