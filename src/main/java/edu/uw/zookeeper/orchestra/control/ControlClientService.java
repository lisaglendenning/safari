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
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.client.NettyClientModule;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ControlClientService<C extends Connection<? super Operation.Request>> extends ClientConnectionExecutorService<C> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(ControlConfiguration.module());
        }

        @Provides @Singleton
        public ConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getConnectionFactory(
                ControlConfiguration configuration,
                RuntimeModule runtime, 
                NettyClientModule clientModule) {
            ParameterizedFactory<Publisher, Pair<Class<Operation.Request>, AssignXidCodec>> codecFactory = ClientApplicationModule.codecFactory();
            ParameterizedFactory<Pair<Pair<Class<Operation.Request>, AssignXidCodec>, Connection<Operation.Request>>, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> pingingFactory = 
                    PingingClient.factory(configuration.getTimeOut(), runtime.executors().asScheduledExecutorServiceFactory().get());
            ClientConnectionFactory<Operation.Request, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> clientConnections = 
                    clientModule.get(codecFactory, pingingFactory).get();
            runtime.serviceMonitor().addOnStart(clientConnections);
            EnsembleViewFactory<ServerInetAddressView, PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> factory = EnsembleViewFactory.newInstance(
                    clientConnections,
                    ServerInetAddressView.class, 
                    configuration.getEnsemble(), 
                    configuration.getTimeOut());
            ConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> instance = new ConnectionFactory<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>(clientConnections, factory);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }

        @Provides @Singleton
        public <C extends Connection<? super Operation.Request>> ControlClientService<C> getControlClientService(
                ConnectionFactory<C> factory,
                RuntimeModule runtime) {
            ControlClientService<C> instance = new ControlClientService<C>(factory);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }

    public static void createPrefix(Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
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
        
        Materializer.Operator<?,?> operator = materializer.operator();
        while (iterator.hasNext()) {
            Schema.SchemaNode node = iterator.next();
            Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = operator.exists(node.path()).submit().get();
            Operation.Response response = Operations.maybeError(result.second().response(), KeeperException.Code.NONODE, result.toString());
            if (response instanceof Operation.Error) {
                result = operator.create(node.path()).submit().get();
                response = Operations.maybeError(result.second().response(), KeeperException.Code.NODEEXISTS, result.toString());
            }
        }
    }

    protected final Materializer<Operation.SessionRequest, Operation.SessionResponse> materializer;
    protected final WatchPromiseTrie watches;

    protected ControlClientService(
            ConnectionFactory<C> factory) {
        super(factory);
        this.materializer = Materializer.newInstance(
                        Control.getSchema(),
                        Control.getByteCodec(),
                        this, 
                        this);
        this.watches = WatchPromiseTrie.newInstance();
    }
    
    public Materializer<Operation.SessionRequest, Operation.SessionResponse> materializer() {
        return materializer;
    }
    
    public EnsembleView<ServerInetAddressView> view() {
        return factory().view();
    }

    public WatchPromiseTrie watches() {
        return watches;
    }
    
    @Override
    protected ConnectionFactory<C> factory() {
        return (ConnectionFactory<C>) factory;
    }

    @Override
    protected void startUp() throws Exception {
        factory().start().get();
        
        super.startUp();

        this.register(watches);
        WatchEventPublisher.newInstance(this, this);
        
        createPrefix(materializer());
    }

    protected static class ConnectionFactory<C extends Connection<? super Operation.Request>> extends AbstractIdleService implements Factory<ClientConnectionExecutor<C>> {

        protected final ClientConnectionFactory<?,C> clientConnections;
        protected final EnsembleViewFactory<ServerInetAddressView, C> factory;
        
        protected ConnectionFactory(
                ClientConnectionFactory<?, C> clientConnections,
                EnsembleViewFactory<ServerInetAddressView, C> factory) {
            this.clientConnections = clientConnections;
            this.factory = factory;
        }
        
        public ClientConnectionFactory<?,C> clientConnections() {
            return clientConnections;
        }
        
        public EnsembleView<ServerInetAddressView> view() {
            return factory.view();
        }
        
        @Override
        public ClientConnectionExecutor<C> get() {
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
