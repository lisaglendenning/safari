package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientProtocolExecutorsService;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.TimeValue;

public class BackendClientService extends ClientProtocolExecutorsService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public BackendConfiguration getBackendConfiguration(RuntimeModule runtime) throws Exception {
            return BackendConfiguration.fromRuntime(runtime);
        }

        @Provides @Singleton
        public ConnectionFactory getConnectionFactory(
                BackendConfiguration configuration,
                RuntimeModule runtime, 
                NettyClientModule clientModule) throws Exception {
            TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance("Backend").get(runtime.configuration());
            AssignXidProcessor xids = AssignXidProcessor.newInstance();
            ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections = clientModule.get(
                    PingingClientCodecConnection.codecFactory(), 
                    PingingClientCodecConnection.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get())).get();
            ServerViewFactory factory = ServerViewFactory.newInstance(
                    clientConnections,
                    xids, 
                    configuration.get().getClientAddress(), 
                    timeOut);
            runtime.serviceMonitor().addOnStart(clientConnections);
            ConnectionFactory instance = new ConnectionFactory(clientConnections, factory, configuration.get());
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }

        @Provides @Singleton
        public BackendClientService getBackendClientService(
                ConnectionFactory factory, 
                ServiceLocator locator,
                RuntimeModule runtime) throws Exception {
            BackendClientService instance = new BackendClientService(factory, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    protected final ServiceLocator locator;
    
    protected BackendClientService(
            ConnectionFactory factory,
            ServiceLocator locator) {
        super(factory);
        this.locator = locator;
    }

    @Override
    public ConnectionFactory factory() {
        return (ConnectionFactory) clientFactory;
    }
    
    public BackendView view() {
        return factory().view();
    }
    
    protected void register() throws InterruptedException, ExecutionException, KeeperException {
        Materializer materializer = locator.getInstance(ControlClientService.class).materializer();
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.of(locator.getInstance(ConductorService.class).view().id());
        Orchestra.Conductors.Entity.Backend backendNode = Orchestra.Conductors.Entity.Backend.create(view(), entityNode, materializer);
        if (! view().equals(backendNode.get())) {
            throw new IllegalStateException(backendNode.get().toString());
        }
    }
    
    @Override
    protected void startUp() throws Exception {
        factory().start().get();
        
        super.startUp();
        
        register();
    }
    
    protected static class ConnectionFactory extends AbstractIdleService implements Factory<ClientProtocolExecutor> {
        
        protected final ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections;
        protected final ServerViewFactory factory;
        protected final BackendView view;
        
        protected ConnectionFactory(
                ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections,
                ServerViewFactory factory,
                BackendView view) {
            this.clientConnections = clientConnections;
            this.factory = factory;
            this.view = view;
        }
        
        public ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections() {
            return clientConnections;
        }
        
        public BackendView view() {
            return view;
        }
        
        @Override
        public ClientProtocolExecutor get() {
            return factory.get();
        }

        @Override
        protected void startUp() throws Exception {
            clientConnections.start().get();
        }

        @Override
        protected void shutDown() throws Exception {
        }
    }
}
