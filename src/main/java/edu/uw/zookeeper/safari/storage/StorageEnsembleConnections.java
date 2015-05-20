package edu.uw.zookeeper.safari.storage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.Cfg;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;

public class StorageEnsembleConnections extends AbstractModule {

    public static StorageEnsembleConnections create() {
        return new StorageEnsembleConnections();
    }
    
    protected StorageEnsembleConnections() {}
    
    @Override
    protected void configure() {
    }

    @Provides @Storage @Singleton
    public EnsembleView<ServerInetAddressView> getStorageEnsembleViewConfiguration(
            StorageCfgFile cfg,
            Configuration configuration) {
        EnsembleView<ServerInetAddressView> ensemble;
        if (cfg.get().isPresent()) {
            ensemble = StorageEnsembleViewConfiguration.fromProperties(cfg.get().get());
        } else {
            ensemble = StorageEnsembleViewConfiguration.get(configuration);
        }
        LogManager.getLogger(getClass()).info("Storage ensemble is {}", ensemble);
        return ensemble;
    }

    @Provides @Storage @Singleton
    public TimeValue getStorageTimeOutConfiguration(
            Configuration configuration) {
        return StorageTimeOutConfiguration.get(configuration);
    }
    
    @Provides @Storage @Singleton
    public ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> newStorageClientConnectionFactory(                
            RuntimeModule runtime,
            @Storage TimeValue timeOut,
            NetClientModule clientModule) {
        ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> instance = ClientConnectionFactoryBuilder.defaults()
                .setClientModule(clientModule)
                .setTimeOut(timeOut)
                .setRuntimeModule(runtime)
                .build();
        runtime.getServiceMonitor().add(instance);
        return instance;
    }

    @Provides @Storage @Singleton
    public EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> newStorageEnsembleClientFactory(
            @Storage EnsembleView<ServerInetAddressView> ensemble,
            @Storage TimeValue timeOut,
            @Storage ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
            ScheduledExecutorService scheduler) {
        EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> factory = EnsembleViewFactory.fromSession(
                connections,
                ensemble, 
                timeOut,
                scheduler);
        return factory;
    }

    @Configurable(path="storage", arg="ensemble", help="address:port,...")
    public static abstract class StorageEnsembleViewConfiguration {

        public static EnsembleView<ServerInetAddressView> fromProperties(Properties properties) {
            ImmutableSet.Builder<ServerInetAddressView> servers = ImmutableSet.builder();
            for (Enumeration<?> names = properties.propertyNames(); names.hasMoreElements();) {
                String k = (String) names.nextElement();
                Matcher m = Cfg.Server.matcher(k);
                if (m.matches()) {
                    String v = properties.getProperty(k);
                    ServerInetAddressView server;
                    if (Cfg.Key.DYNAMIC_CONFIG_FILE.hasValue(properties)) {
                        Cfg.DynamicServer value = Cfg.DynamicServer.valueOf(v);
                        try {
                            server = ServerInetAddressView.fromString(
                                    String.format("%s:%d", value.getAddress(), 
                                            value.getClientPort()));
                        } catch (UnknownHostException e) {
                            throw new IllegalArgumentException(e);
                        }
                    } else {
                        // note we assume that (1) clientPort is specified,
                        // (2) all servers have the same port
                        int clientPort = Integer.parseInt(Cfg.Key.CLIENT_PORT.getValue(properties));
                        Cfg.StaticServer value = Cfg.StaticServer.valueOf(v);
                        try {
                            if (value.getHostname() == Cfg.StaticServer.DEFAULT_HOSTNAME) {
                                server = ServerInetAddressView.of(
                                        InetAddress.getByName("127.0.0.1"), 
                                        clientPort);
                            } else {
                                server = ServerInetAddressView.fromString(
                                        String.format("%s:%d", value.getHostname(), 
                                                clientPort));
                            }
                        } catch (UnknownHostException e) {
                            throw new IllegalArgumentException(e);
                        }
                    }
                    servers.add(server);
                }
            }
            EnsembleView<ServerInetAddressView> ensemble = EnsembleView.copyOf(servers.build());
            if (ensemble.isEmpty()) {
                // assume static standalone
                ServerInetAddressView server = StorageServerConnections.StorageServerAddressConfiguration.fromProperties(properties);
                ensemble = EnsembleView.copyOf(server);
            }
            return ensemble;
        }
        
        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            return (config.hasPath(configurable.arg())) ? ServerInetAddressView.ensembleFromString(
                        config.getString(configurable.arg())) : null;
        }
        
        public static Configuration set(Configuration configuration, EnsembleView<ServerInetAddressView> value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(
                    ConfigFactory.parseMap(
                            ImmutableMap.<String,Object>builder()
                            .put(ConfigUtil.joinPath(configurable.path(), configurable.arg()), 
                                    EnsembleView.toString(value)).build()));
        }
        
        public static Configurable getConfigurable() {
            return StorageEnsembleViewConfiguration.class.getAnnotation(Configurable.class);
        }
        
        protected StorageEnsembleViewConfiguration() {}
    }

    @Configurable(path="storage", key="timeout", value="30 seconds", help="time")
    public static abstract class StorageTimeOutConfiguration {

        public static TimeValue get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            return TimeValue.fromString(
                    configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                    .getString(configurable.key()));
        }
        
        public static Configuration set(Configuration configuration, TimeValue value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(
                    ConfigFactory.parseMap(
                            ImmutableMap.<String,Object>builder()
                            .put(ConfigUtil.joinPath(configurable.path(), configurable.key()), 
                                    value.toString()).build()));
        }
        
        public static Configurable getConfigurable() {
            return StorageTimeOutConfiguration.class.getAnnotation(Configurable.class);
        }
        
        protected StorageTimeOutConfiguration() {}
    }
}
