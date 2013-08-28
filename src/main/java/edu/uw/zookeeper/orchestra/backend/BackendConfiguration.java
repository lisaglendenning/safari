package edu.uw.zookeeper.orchestra.backend;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerRoleView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.DefaultsFactory;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.jmx.ServerViewJmxQuery;
import edu.uw.zookeeper.jmx.SunAttachQueryJmx;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.control.ControlSchema;

public class BackendConfiguration {

    public static com.google.inject.Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public BackendConfiguration getBackendConfiguration(
                Configuration configuration) throws Exception {
            ServerInetAddressView clientAddress = BackendAddressDiscovery.call(configuration);
            EnsembleView<ServerInetAddressView> ensemble = BackendEnsembleViewFactory.getInstance(configuration);
            TimeValue timeOut = ConfigurableTimeout.get(configuration);
            return new BackendConfiguration(BackendView.of(clientAddress, ensemble), timeOut);
        }
    }

    public static void advertise(Identifier myEntity, BackendView view, Materializer<?> materializer) throws InterruptedException, ExecutionException, KeeperException {
        ControlSchema.Peers.Entity entityNode = ControlSchema.Peers.Entity.of(myEntity);
        ControlSchema.Peers.Entity.Backend valueNode = ControlSchema.Peers.Entity.Backend.create(view, entityNode, materializer).get();
        if (! view.equals(valueNode.get())) {
            throw new IllegalStateException(String.valueOf(valueNode.get()));
        }
    }
    
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
    
    protected static abstract class OptionalConfiguration implements Function<Configuration, String> {

        @Override
        public String apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                return config.getString(configurable.key());
            } else {
                return null;
            }     
        }
    }

    @Configurable(path="Backend", key="Timeout", value="30 seconds", help="Time")
    public static class ConfigurableTimeout extends DefaultMain.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }
    }
    
    @Configurable(path="Backend", key="ClientAddress", arg="backend", help="Address:Port")
    public static class ConfigurableAddressView extends OptionalConfiguration {
    
        public static String get(Configuration configuration) {
            return new ConfigurableAddressView().apply(configuration);
        }
    }

    @Configurable(path="Backend", key="Ensemble", arg="ensemble", help="Address:Port,...")
    public static class ConfigurableEnsembleView extends OptionalConfiguration {
    
        public static String get(Configuration configuration) {
            return new ConfigurableEnsembleView().apply(configuration);
        }
    }

    public static class BackendAddressDiscovery implements Callable<ServerInetAddressView> {
        
        public static ServerInetAddressView call(Configuration configuration) throws Exception {
            BackendAddressDiscovery instance = new BackendAddressDiscovery(configuration);
            return instance.call();
        }
        
        public static ServerInetAddressView query() throws IOException {
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
                if (connector != null) {
                    connector.close();
                }
            }
        }
        
        protected final Logger logger = LogManager.getLogger();
        protected final Configuration configuration;
        
        public BackendAddressDiscovery(Configuration configuration) {
            this.configuration = configuration;
        }
        
        @Override
        public ServerInetAddressView call() throws Exception {
            String configured = ConfigurableAddressView.get(configuration);
            if (configured != null) {
                return ServerInetAddressView.fromString(configured);
            }
            // If the backend server is not actively serving (i.e. in leader election),
            // then it doesn't advertise it's clientAddress over JMX
            // So poll until I can discover the backend client address
            ServerInetAddressView backend = null;
            long backoff = 1000;
            while (backend == null) {
                backend = query();
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

    public static class BackendEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleView<ServerInetAddressView>> {

        public static EnsembleView<ServerInetAddressView> getInstance(Configuration configuration) {
            return new BackendEnsembleViewFactory().get(configuration);
        }
        
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
            String configured = ConfigurableEnsembleView.get(value);
            if (configured != null) {
                return EnsembleView.fromString(configured);
            } else {
                return get();
            }
        }
    }
}
