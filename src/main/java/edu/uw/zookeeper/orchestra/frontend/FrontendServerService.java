package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.ConductorService;
import edu.uw.zookeeper.orchestra.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
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

public class FrontendServerService extends AbstractIdleService {

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
                            ServerApplicationModule.connectionFactory()).get(configuration.get().get());
            runtime.serviceMonitor().addOnStart(serverConnections);
            ServerConnectionListener.newInstance(serverConnections, serverExecutor, serverExecutor, serverExecutor);
            FrontendServerService instance = new FrontendServerService(configuration.get(), serverConnections, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    protected final ServiceLocator locator;
    protected final ServerInetAddressView address;
    protected final ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections;
    
    protected FrontendServerService(
            ServerInetAddressView address,
            ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections,
            ServiceLocator locator) {
        this.address = address;
        this.serverConnections = serverConnections;
        this.locator = locator;
    }
    
    public ServerInetAddressView address() {
        return address;
    }
    
    public ServerConnectionFactory<Message.Server, ProtocolCodecConnection<Message.Server, ServerProtocolCodec, Connection<Message.Server>>> serverConnections() {
        return serverConnections;
    }
    
    public void register() throws InterruptedException, ExecutionException, KeeperException {
        Materializer<?,?> materializer = locator.getInstance(ControlClientService.class).materializer();
        Orchestra.Peers.Entity entityNode = Orchestra.Peers.Entity.of(locator.getInstance(ConductorService.class).view().id());
        Orchestra.Peers.Entity.ClientAddress clientAddressNode = Orchestra.Peers.Entity.ClientAddress.create(address(), entityNode, materializer);
        if (! address().equals(clientAddressNode.get())) {
            throw new IllegalStateException(clientAddressNode.get().toString());
        }        
    }

    @Override
    protected void startUp() throws Exception {
        serverConnections().start().get();
        
        register();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
