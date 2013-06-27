package edu.uw.zookeeper.orchestra.control;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerView;
import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientProtocolExecutorService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.TimeValue;

public class ControlClientService extends ClientProtocolExecutorService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public ControlConfiguration getControlConfiguration(RuntimeModule runtime) {
            return ControlConfiguration.fromRuntime(runtime);
        }

        @Provides @Singleton
        public ConnectionFactory getConnectionFactory(
                ControlConfiguration configuration,
                RuntimeModule runtime, 
                NettyClientModule clientModule) {
            TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance("Control").get(runtime.configuration());
            AssignXidProcessor xids = AssignXidProcessor.newInstance();
            ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections = clientModule.get(
                    PingingClientCodecConnection.codecFactory(), 
                    PingingClientCodecConnection.factory(timeOut, runtime.executors().asScheduledExecutorServiceFactory().get())).get();
            runtime.serviceMonitor().addOnStart(clientConnections);
            EnsembleViewFactory factory = EnsembleViewFactory.newInstance(
                    clientConnections,
                    xids, 
                    configuration.get(), 
                    timeOut);
            ConnectionFactory instance = new ConnectionFactory(clientConnections, factory);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }

        @Provides @Singleton
        public ControlClientService getControlClientService(
                ConnectionFactory factory,
                RuntimeModule runtime) {
            ControlClientService instance = new ControlClientService(factory);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }

    public static void createPrefix(Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
        // The prefix is small enough that there's no need to get fancy here
        final Predicate<Schema.SchemaNode> isPrefix = new Predicate<Schema.SchemaNode>() {
            @Override
            public boolean apply(Schema.SchemaNode input) {
                return (LabelType.LABEL == input.get().getLabelType());
            }            
        };
        
        final Iterator<Schema.SchemaNode> iterator = new ZNodeLabelTrie.BreadthFirstTraversal<Schema.SchemaNode>(materializer.schema().root()) {
            @Override
            protected Iterable<Schema.SchemaNode> childrenOf(Schema.SchemaNode node) {
                return Iterables.filter(node.values(), isPrefix);
            }
        };
        
        Materializer.Operator operator = materializer.operator();
        while (iterator.hasNext()) {
            Schema.SchemaNode node = iterator.next();
            Operation.SessionResult result = operator.exists(node.path()).submit().get();
            Operation.Response reply = Operations.maybeError(result.reply().reply(), KeeperException.Code.NONODE, result.toString());
            if (reply instanceof Operation.Error) {
                result = operator.create(node.path()).submit().get();
                reply = Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
            }
        }
    }

    protected final Materializer materializer;
    protected final WatchPromiseTrie watches;

    protected ControlClientService(
            ConnectionFactory factory) {
        super(factory);
        this.materializer = Materializer.newInstance(
                        Control.getSchema(),
                        Control.getByteCodec(),
                        this, 
                        this);
        this.watches = WatchPromiseTrie.newInstance();
    }
    
    public Materializer materializer() {
        return materializer;
    }
    
    public EnsembleView<? extends ServerView.Address<?>> view() {
        return factory().view();
    }

    public WatchPromiseTrie watches() {
        return watches;
    }
    
    @Override
    protected ConnectionFactory factory() {
        return (ConnectionFactory) factory;
    }

    @Override
    protected void startUp() throws Exception {
        factory().start().get();
        
        super.startUp();

        this.register(watches);
        WatchEventPublisher.newInstance(this, this);
        
        createPrefix(materializer());
    }

    protected static class ConnectionFactory extends AbstractIdleService implements Factory<ClientProtocolExecutor> {

        protected final ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections;
        protected final EnsembleViewFactory factory;
        
        protected ConnectionFactory(
                ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections,
                EnsembleViewFactory factory) {
            this.clientConnections = clientConnections;
            this.factory = factory;
        }
        
        public ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections() {
            return clientConnections;
        }
        
        public EnsembleView<? extends ServerView.Address<?>> view() {
            return factory.view();
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
