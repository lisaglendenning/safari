package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientProtocolExecutorsService;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.TimeValue;

public class BackendClientService extends ClientProtocolExecutorsService {

    public static BackendClientService newInstance(ServiceManager manager) throws Exception {
        BackendView view = BackendConfiguration.get(manager.runtime());
        ConnectionFactory factory = ConnectionFactory.newInstance(view, manager.runtime(), manager.netModule().clients());
        manager.runtime().serviceMonitor().addOnStart(factory);
        return new BackendClientService(factory, manager);
    }

    protected final ServiceManager manager;
    
    protected BackendClientService(
            ConnectionFactory factory,
            ServiceManager manager) {
        super(factory);
        this.manager = manager;
    }

    @Override
    public ConnectionFactory factory() {
        return (ConnectionFactory) clientFactory;
    }
    
    public BackendView view() {
        return factory().view();
    }
    
    protected void register() throws InterruptedException, ExecutionException, KeeperException {
        Materializer materializer = manager.controlClient().materializer();
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.of(manager.conductor().view().id());
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
    
    public static class ConnectionFactory extends AbstractIdleService implements Factory<ClientProtocolExecutor> {
        
        public static ConnectionFactory newInstance(
                BackendView view,
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
                    view.getClientAddress(), 
                    timeOut);
            runtime.serviceMonitor().addOnStart(clientConnections);
            return new ConnectionFactory(clientConnections, factory, view);
        }
        
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
