package edu.uw.zookeeper.safari.backend;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlSchema;

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
            BackendView view = ConfigurableBackendCfg.get(configuration);
            if (view == null) {
                ServerInetAddressView clientAddress = ConfigurableAddressView.get(configuration);
                if (clientAddress == null) {
                    clientAddress = BackendAddressDiscovery.get();
                }
                EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(configuration);
                if (ensemble == null) {
                    ensemble = BackendEnsembleViewDiscovery.get();
                }
                view = BackendView.of(clientAddress, ensemble);
            }
            TimeValue timeOut = ConfigurableTimeout.get(configuration);
            BackendConfiguration instance = new BackendConfiguration(view, timeOut);
            LogManager.getLogger(getClass()).info("{}", instance);
            return instance;
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
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("view", view).add("timeOut", timeOut).toString();
    }
    
    @Configurable(path="backend", key="timeout", value="30 seconds", help="time")
    public static class ConfigurableTimeout extends ZooKeeperApplication.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }
    }
    
    @Configurable(path="backend", key="clientAddress", arg="backend", help="address:port")
    public static class ConfigurableAddressView implements Function<Configuration, ServerInetAddressView> {
    
        public static ServerInetAddressView get(Configuration configuration) {
            return new ConfigurableAddressView().apply(configuration);
        }

        @Override
        public ServerInetAddressView apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                try {
                    return ServerInetAddressView.fromString(config.getString(configurable.key()));
                } catch (UnknownHostException e) {
                    throw new IllegalArgumentException(e);
                }
            } else {
                return null;
            }     
        }
    }

    @Configurable(path="backend", key="ensemble", arg="ensemble", help="address:port,...")
    public static class ConfigurableEnsembleView implements Function<Configuration, EnsembleView<ServerInetAddressView>> {
    
        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            return new ConfigurableEnsembleView().apply(configuration);
        }
        
        @Override
        public EnsembleView<ServerInetAddressView> apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                return EnsembleView.fromString(
                        config.getString(configurable.key()));
            } else {
                return null;
            }     
        }
    }

    @Configurable(path="backend", key="cfg", arg="backendCfg", help="zoo.cfg")
    public static class ConfigurableBackendCfg implements Function<Configuration, BackendView> {
    
        public static BackendView get(Configuration configuration) {
            return new ConfigurableBackendCfg().apply(configuration);
        }
        
        @Override
        public BackendView apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            if (config.hasPath(configurable.key())) {
                try {
                    FileInputStream cfg = new FileInputStream(
                            new File(config.getString(configurable.key())));
                    Properties properties = new Properties();
                    try {
                        properties.load(cfg);
                    } finally {
                        cfg.close();
                    }
                    
                    ServerInetAddressView clientAddress;
                    int clientPort = Integer.parseInt(properties.getProperty("clientPort"));
                    String clientPortAddress = properties.getProperty("clientPortAddress", "").trim();
                    if (clientPortAddress.isEmpty()) {
                        clientAddress = ServerInetAddressView.of(
                                InetAddress.getByName("127.0.0.1"), clientPort);
                    } else {
                        checkArgument(clientPort > 0);
                        clientAddress = ServerInetAddressView.fromString(
                                String.format("%s:%d", clientPortAddress, clientPort));
                    }
                    
                    Set<Map.Entry<Object, Object>> serverProperties = Sets.filter(properties.entrySet(),
                            new Predicate<Map.Entry<Object, Object>>(){

                                @Override
                                public boolean apply(Map.Entry<Object, Object> input) {
                                    // TODO Auto-generated method stub
                                    return input.getKey().toString().trim().startsWith("server.");
                                }});
                    ImmutableSet.Builder<ServerInetAddressView> servers = ImmutableSet.builder();
                    for (Map.Entry<Object, Object> e: serverProperties) {
                        String[] fields = e.getValue().toString().trim().split(":");
                        ServerInetAddressView server;
                        if (fields.length == 1) {
                            server = ServerInetAddressView.of(
                                    InetAddress.getByName("127.0.0.1"), 
                                    Integer.parseInt(fields[0]));
                        } else if (fields.length >= 2) {
                            server = ServerInetAddressView.fromString(
                                    String.format("%s:%s", fields[0], fields[1]));
                        } else {
                            throw new IllegalArgumentException(String.valueOf(e));
                        }
                        servers.add(server);
                    }
                    EnsembleView<ServerInetAddressView> ensemble = EnsembleView.from(servers.build());
                    
                    return BackendView.of(clientAddress, ensemble);
                } catch (Exception e) {
                    Throwables.propagateIfInstanceOf(e, IllegalArgumentException.class); 
                    throw new IllegalArgumentException(e);
                }
            } else {
                return null;
            }     
        }
    }
}
