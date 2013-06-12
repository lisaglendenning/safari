package edu.uw.zookeeper.orchestra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;

import edu.uw.zookeeper.EnsembleRoleView;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ServerRoleView;
import edu.uw.zookeeper.client.ClientProtocolExecutorsService;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.jmx.ServerViewJmxQuery;
import edu.uw.zookeeper.jmx.SunAttachQueryJmx;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;

public class BackendService extends ClientProtocolExecutorsService {

    public static class BackendAddressDiscovery implements Callable<ServerInetAddressView> {

        protected final Logger logger = LoggerFactory.getLogger(BackendAddressDiscovery.class);
        protected final RuntimeModule runtime;
        
        public BackendAddressDiscovery(RuntimeModule runtime) {
            this.runtime = runtime;
        }
        
        @Override
        public ServerInetAddressView call() throws Exception {
            // If the backend server is not actively serving (i.e. in leader election),
            // then it doesn't advertise it's clientAddress over JMX
            // So poll until I can discover the backend client address
            ServerInetAddressView backend = null;
            long backoff = 1000;
            while (backend == null) {
                backend = BackendAddressViewFactory.getInstance().get(runtime.configuration());
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
    
    public static BackendService newInstance(
            RuntimeModule runtime, ClientModule clientModule) throws InterruptedException, ExecutionException {
        BackendAddressDiscovery discovery = new BackendAddressDiscovery(runtime);
        ServerInetAddressView clientAddress = 
                runtime.executors().asExecutorServiceFactory().get().submit(discovery).get();
        ServerViewFactory clientFactory = ServerViewFactory.newInstance(
                clientModule.clientConnections(),
                clientModule.xids(), 
                clientAddress, 
                clientModule.timeOut());

        EnsembleRoleView<InetSocketAddress, ServerInetAddressView> ensembleView = BackendEnsembleViewFactory.getInstance().get(runtime.configuration());
        SortedSet<ServerInetAddressView> ensemble = 
                ImmutableSortedSet.copyOf(Iterables.transform(ensembleView, new Function<ServerRoleView<InetSocketAddress, ServerInetAddressView>, ServerInetAddressView>() {
                    @Override
                    @Nullable
                    public ServerInetAddressView apply(
                            ServerRoleView<InetSocketAddress, ServerInetAddressView> input) {
                        return input.first();
                    }
                    
                }));
        BackendView view = BackendView.of(clientAddress, EnsembleView.from(ensemble));
        return new BackendService(clientFactory, view);
    }
    
    public static enum BackendAddressViewFactory implements DefaultsFactory<Configuration, ServerInetAddressView> {
        INSTANCE;
        
        public static BackendAddressViewFactory getInstance() {
            return INSTANCE;
        }

        public static final String ARG = "backend";
        public static final String CONFIG_KEY = "Backend";
        public static final String CONFIG_PATH = "";
        
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
    
    public static enum BackendEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleRoleView<InetSocketAddress, ServerInetAddressView>> {
        INSTANCE;
        
        public static BackendEnsembleViewFactory getInstance() {
            return INSTANCE;
        }
        
        public static final String ARG = "ensemble";
        public static final String CONFIG_KEY = "Ensemble";
        public static final String CONFIG_PATH = "";
        
        @Override
        public EnsembleRoleView<InetSocketAddress, ServerInetAddressView> get() {        
            DefaultsFactory<String, JMXServiceURL> urlFactory = SunAttachQueryJmx.getInstance();
            JMXServiceURL url = urlFactory.get();
            JMXConnector connector = null;
            try {
                connector = JMXConnectorFactory.connect(url);
                MBeanServerConnection mbeans = connector.getMBeanServerConnection();
                return ServerViewJmxQuery.ensembleViewOf(mbeans);
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
        public EnsembleRoleView<InetSocketAddress, ServerInetAddressView> get(Configuration value) {
            Arguments arguments = value.asArguments();
            if (! arguments.has(ARG)) {
                arguments.add(arguments.newOption(ARG, "Ensemble"));
            }
            arguments.parse();
            Map.Entry<String, String> args = new AbstractMap.SimpleImmutableEntry<String,String>(ARG, CONFIG_KEY);
            Config config = value.withArguments(CONFIG_PATH, args);
            if (config.hasPath(CONFIG_KEY)) {
                String input = config.getString(CONFIG_KEY);
                return EnsembleRoleView.fromStringRoles(input);
            } else {
                return get();
            }
        }
    }
    
    protected final BackendView view;

    protected BackendService(ServerViewFactory clientFactory, 
            BackendView view) {
        super(clientFactory);
        this.view = view;
    }
    
    public BackendView view() {
        return view;
    }
}
