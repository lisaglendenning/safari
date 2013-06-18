package edu.uw.zookeeper.orchestra.control;

import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;

import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerRoleView;
import edu.uw.zookeeper.client.ClientProtocolExecutorService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.orchestra.ClientConnectionsModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.PingingClientCodecConnection;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;

public class ControlClientService extends ClientProtocolExecutorService {

    public static ControlClientService newInstance(
            RuntimeModule runtime, ClientConnectionsModule clientModule) {
        ClientConnectionFactory<Message.ClientSessionMessage, PingingClientCodecConnection> clientConnections = clientModule.clientConnections();
        runtime.serviceMonitor().add(clientConnections);

        EnsembleRoleView<InetSocketAddress, ServerInetAddressView> controlView  = ControlEnsembleViewFactory.getInstance().get(runtime.configuration());
        EnsembleViewFactory controlFactory = EnsembleViewFactory.newInstance(
                clientConnections,
                clientModule.xids(), 
                controlView, 
                clientModule.timeOut());
        return new ControlClientService(controlFactory);
    }
    
    public static enum ControlEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleRoleView<InetSocketAddress, ServerInetAddressView>> {
        INSTANCE;
        
        public static ControlEnsembleViewFactory getInstance() {
            return INSTANCE;
        }
        
        public static final String ARG = "control";
        public static final String CONFIG_KEY = "Control";

        public static final String DEFAULT_ADDRESS = "localhost";
        public static final int DEFAULT_PORT = 2381;
        
        public static final String CONFIG_PATH = "";
        
        @SuppressWarnings("unchecked")
        @Override
        public EnsembleRoleView<InetSocketAddress, ServerInetAddressView> get() {
            return EnsembleRoleView.ofRoles(
                    ServerRoleView.of(ServerInetAddressView.of(
                    DEFAULT_ADDRESS, DEFAULT_PORT)));
        }

        @Override
        public EnsembleRoleView<InetSocketAddress, ServerInetAddressView> get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(ARG)) {
                arguments.add(arguments.newOption(ARG, "Ensemble"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(ARG, CONFIG_KEY);
            @SuppressWarnings("unchecked")
            Config config = value.withArguments(CONFIG_PATH, args);
            if (config.hasPath(CONFIG_KEY)) {
                String input = config.getString(CONFIG_KEY);
                return EnsembleRoleView.fromStringRoles(input);
            } else {
                return get();
            }
        }
    }
    
    protected final EnsembleViewFactory view;
    protected final Materializer materializer;

    protected ControlClientService(
            EnsembleViewFactory view) {
        super(view);
        this.view = view;
        this.materializer = Materializer.newInstance(
                        Control.getSchema(),
                        Control.getByteCodec(),
                        this, 
                        this);
    }
    
    public Materializer materializer() {
        return materializer;
    }
    
    public EnsembleViewFactory view() {
        return view;
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();

        createPrefix();
    }

    public void createPrefix() throws KeeperException, InterruptedException, ExecutionException {
        // The prefix is small enough that there's no need to get fancy here
        final Predicate<Schema.SchemaNode> isPrefix = new Predicate<Schema.SchemaNode>() {
            @Override
            public boolean apply(Schema.SchemaNode input) {
                return (LabelType.LABEL == input.get().getLabelType());
            }            
        };
        
        final Iterator<Schema.SchemaNode> iterator = new ZNodeLabelTrie.BreadthFirstTraversal<Schema.SchemaNode>(materializer().schema().root()) {
            @Override
            protected Iterable<Schema.SchemaNode> childrenOf(Schema.SchemaNode node) {
                return Iterables.filter(node.values(), isPrefix);
            }
        };
        
        Materializer.Operator operator = materializer().operator();
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
}
