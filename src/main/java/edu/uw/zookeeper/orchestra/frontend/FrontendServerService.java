package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.protocol.server.ServerTaskExecutor;
import edu.uw.zookeeper.server.ServerApplicationModule;

@DependsOn({FrontendServerExecutor.class})
public class FrontendServerService extends DependentService.SimpleDependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(FrontendConfiguration.module());
            install(FrontendServerExecutor.module());
        }

        @Provides @Singleton
        public FrontendServerService getFrontendServerService(
                FrontendConfiguration configuration, 
                ServerTaskExecutor serverExecutor,
                ServiceLocator locator,
                NettyModule netModule,
                DependentServiceMonitor monitor) throws Exception {
            ServerConnectionFactory<ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                    netModule.servers().get(
                            ServerApplicationModule.codecFactory(),
                            ServerApplicationModule.connectionFactory()).get(configuration.getAddress().get());
            monitor.get().addOnStart(serverConnections);
            ServerConnectionExecutorsService<Connection<Message.Server>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> server = ServerConnectionExecutorsService.newInstance(serverConnections, serverExecutor);
            monitor.get().addOnStart(server);
            return monitor.add(
                    FrontendServerService.newInstance(server, locator));
        }
    }
    
    public static FrontendServerService newInstance(
            ServerConnectionExecutorsService<Connection<Message.Server>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections,
            ServiceLocator locator) {
        FrontendServerService instance = new FrontendServerService(serverConnections, locator);
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

    protected final ServerConnectionExecutorsService<Connection<Message.Server>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections;
    
    protected FrontendServerService(
            ServerConnectionExecutorsService<Connection<Message.Server>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections,
            ServiceLocator locator) {
        super(locator);
        this.connections = connections;
    }

    public ServerConnectionExecutorsService<Connection<Message.Server>, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> connections() {
        return connections;
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        // global barrier - wait for all volumes to be assigned
        AllVolumesAssigned.call( 
                locator().getInstance(ControlMaterializerService.class).materializer()).get();

        connections().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        connections().stop().get();
        
        super.shutDown();
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
