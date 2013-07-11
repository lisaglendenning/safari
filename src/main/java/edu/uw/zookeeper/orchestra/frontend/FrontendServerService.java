package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.DependentService;
import edu.uw.zookeeper.orchestra.DependsOn;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.Control;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.peer.EnsemblePeerService;
import edu.uw.zookeeper.orchestra.peer.PeerConfiguration;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.server.ServerProtocolCodec;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpireSessionsTask;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.server.ServerConnectionListener;
import edu.uw.zookeeper.server.ServerExecutor;
import edu.uw.zookeeper.server.SessionParametersPolicy;

@DependsOn({EnsemblePeerService.class})
public class FrontendServerService extends DependentService.SimpleDependentService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(FrontendConfiguration.module());
        }

        @Provides @Singleton
        public ExpiringSessionManager getSessionManager(
                RuntimeModule runtime) {
            SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(runtime.configuration());
            ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(runtime.publisherFactory().get(), policy);
            ExpireSessionsTask expires = ExpireSessionsTask.newInstance(sessions, runtime.executors().asScheduledExecutorServiceFactory().get(), runtime.configuration());   
            runtime.serviceMonitor().add(expires);
            return sessions;
        }

        @Provides @Singleton
        public ServerExecutor getServerExecutor(
                ExpiringSessionManager sessions,
                RuntimeModule runtime) {
            ServerExecutor serverExecutor = ServerExecutor.newInstance(
                    runtime.executors().asListeningExecutorServiceFactory().get(), runtime.publisherFactory(), sessions);
            return serverExecutor;
        }
        
        @Provides @Singleton
        public FrontendServerService getFrontendServerService(
                FrontendConfiguration configuration, 
                ServerExecutor serverExecutor,
                ServiceLocator locator,
                NettyModule netModule,
                RuntimeModule runtime) throws Exception {
            ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections = 
                    netModule.servers().get(
                            ServerApplicationModule.codecFactory(),
                            ServerApplicationModule.connectionFactory()).get(configuration.getAddress().get());
            runtime.serviceMonitor().addOnStart(serverConnections);
            ServerConnectionListener.newInstance(serverConnections, serverExecutor, serverExecutor, serverExecutor);
            FrontendServerService instance = new FrontendServerService(configuration.getAddress(), serverConnections, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public static FrontendServerService newInstance(
            ServerInetAddressView address,
            ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections,
            ServiceLocator locator) {
        FrontendServerService instance = new FrontendServerService(address, serverConnections, locator);
        instance.new Advertiser(MoreExecutors.sameThreadExecutor());
        return instance;
    }
    
    protected final ServerInetAddressView address;
    protected final ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections;
    
    protected FrontendServerService(
            ServerInetAddressView address,
            ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections,
            ServiceLocator locator) {
        super(locator);
        this.address = address;
        this.serverConnections = serverConnections;
    }
    
    public ServerInetAddressView address() {
        return address;
    }
    
    public ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections() {
        return serverConnections;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        final ZNodeLabel.Path VOLUMES_PATH = Control.path(Orchestra.Volumes.class);
        
        // Global barrier - Wait for all volumes to be assigned
        Predicate<Materializer<?,?>> allAssigned = new Predicate<Materializer<?,?>>() {
            @Override
            public boolean apply(@Nullable Materializer<?,?> input) {
                ZNodeLabel.Component label = Orchestra.Volumes.Entity.Ensemble.LABEL;
                boolean done = true;
                for (Materializer.MaterializedNode e: input.get(VOLUMES_PATH).values()) {
                    if (! e.containsKey(label)) {
                        done = false;
                        break;
                    }
                }
                return done;
            }
        };
        Control.FetchUntil.newInstance(VOLUMES_PATH, allAssigned, locator().getInstance(ControlMaterializerService.class).materializer(), MoreExecutors.sameThreadExecutor()).get();

        serverConnections().start().get();
    }

    @Override
    protected void shutDown() throws Exception {
        serverConnections().stop().get();
        
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
            Materializer<?,?> materializer = locator().getInstance(ControlMaterializerService.class).materializer();
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
