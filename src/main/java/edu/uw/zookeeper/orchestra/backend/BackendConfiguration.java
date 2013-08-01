package edu.uw.zookeeper.orchestra.backend;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerRoleView;
import edu.uw.zookeeper.client.ClientApplicationModule;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.jmx.ServerViewJmxQuery;
import edu.uw.zookeeper.jmx.SunAttachQueryJmx;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.TimeValue;

public class BackendConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public BackendConfiguration getBackendConfiguration(Configuration configuration) throws Exception {
            ServerInetAddressView clientAddress = BackendAddressDiscovery.call(configuration);
            EnsembleView<ServerInetAddressView> ensemble = BackendEnsembleViewFactory.getInstance().get(configuration);
            TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance(CONFIG_PATH).get(configuration);
            return new BackendConfiguration(BackendView.of(clientAddress, ensemble), timeOut);
        }
    }

    public static void advertise(Identifier myEntity, BackendView view, Materializer<?, ?> materializer) throws InterruptedException, ExecutionException, KeeperException {
        ControlSchema.Peers.Entity entityNode = ControlSchema.Peers.Entity.of(myEntity);
        ControlSchema.Peers.Entity.Backend backendNode = ControlSchema.Peers.Entity.Backend.create(view, entityNode, materializer).get();
        if (! view.equals(backendNode.get())) {
            throw new IllegalStateException(backendNode.get().toString());
        }
    }
    
    public static final String CONFIG_PATH = "Backend";
    
    private final BackendView view;
    private final TimeValue timeOut;
    
    public BackendConfiguration(BackendView view, TimeValue timeOut) {
        this.view = view;
        this.timeOut = timeOut;
    }
    
    public BackendView getView() {
        return view;
    }
    
    public TimeValue getTimeOut() {
        return timeOut;
    }

    public static class BackendAddressDiscovery implements Callable<ServerInetAddressView> {
        
        public static ServerInetAddressView call(Configuration configuration) throws Exception {
            BackendAddressDiscovery instance = new BackendAddressDiscovery(configuration);
            return instance.call();
        }
        
        protected final Logger logger = LoggerFactory.getLogger(BackendAddressDiscovery.class);
        protected final Configuration configuration;
        
        public BackendAddressDiscovery(Configuration configuration) {
            this.configuration = configuration;
        }
        
        @Override
        public ServerInetAddressView call() throws Exception {
            // If the backend server is not actively serving (i.e. in leader election),
            // then it doesn't advertise it's clientAddress over JMX
            // So poll until I can discover the backend client address
            ServerInetAddressView backend = null;
            long backoff = 1000;
            while (backend == null) {
                backend = BackendAddressViewFactory.getInstance().get(configuration);
                if (backend == null) {
                    logger.debug("Querying backend failed; retrying in {} ms", backoff);
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    }
                    backoff *= 2;
                }
            }
            return backend;
        }
        
    }

    public static enum BackendAddressViewFactory implements DefaultsFactory<Configuration, ServerInetAddressView> {
        INSTANCE;
        
        public static BackendAddressViewFactory getInstance() {
            return INSTANCE;
        }
    
        public static final String ARG = "backend";
        public static final String CONFIG_KEY = "ClientAddress";
        
        @Override
        public ServerInetAddressView get() {
            DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
            JMXServiceURL url = urlFactory.get();
            JMXConnector connector = null;
            try {
                connector = JMXConnectorFactory.connect(url);
                MBeanServerConnection mbeans = connector.getMBeanServerConnection();
                return ServerViewJmxQuery.addressViewOf(mbeans);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            } finally {
                try {
                    if (connector != null) {
                        connector.close();
                    }
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    
        @Override
        public ServerInetAddressView get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(ARG)) {
                arguments.add(arguments.newOption(ARG, "Address"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(ARG, CONFIG_KEY);
            @SuppressWarnings("unchecked")
            Config config = value.withArguments(CONFIG_PATH, args);
            if (config.hasPath(CONFIG_KEY)) {
                String input = config.getString(CONFIG_KEY);
                return ServerInetAddressView.fromString(input);
            } else {
                return get();
            }
        }
    }

    public static enum BackendEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleView<ServerInetAddressView>> {
        INSTANCE;
        
        public static BackendEnsembleViewFactory getInstance() {
            return INSTANCE;
        }
        
        public static final String ARG = "ensemble";
        public static final String CONFIG_KEY = "Ensemble";
        
        @Override
        public EnsembleView<ServerInetAddressView> get() {        
            DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
            JMXServiceURL url = urlFactory.get();
            JMXConnector connector = null;
            try {
                connector = JMXConnectorFactory.connect(url);
                MBeanServerConnection mbeans = connector.getMBeanServerConnection();
                EnsembleRoleView<InetSocketAddress, ServerInetAddressView> roles = ServerViewJmxQuery.ensembleViewOf(mbeans);
                return EnsembleView.from(ImmutableSortedSet.copyOf(Iterators.transform(
                        roles.iterator(), 
                        new Function<ServerRoleView<InetSocketAddress, ServerInetAddressView>, ServerInetAddressView>() {
                            @Override
                            public ServerInetAddressView apply(
                                    ServerRoleView<InetSocketAddress, ServerInetAddressView> input) {
                                return input.first();
                            }
                        })));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            } finally {
                try {
                    if (connector != null) {
                        connector.close();
                    }
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    
        @SuppressWarnings("unchecked")
        @Override
        public EnsembleView<ServerInetAddressView> get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(ARG)) {
                arguments.add(arguments.newOption(ARG, "Ensemble"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(ARG, CONFIG_KEY);
            Config config = value.withArguments(CONFIG_PATH, args);
            if (config.hasPath(CONFIG_KEY)) {
                String input = config.getString(CONFIG_KEY);
                return EnsembleView.fromString(input);
            } else {
                return get();
            }
        }
    }
}
