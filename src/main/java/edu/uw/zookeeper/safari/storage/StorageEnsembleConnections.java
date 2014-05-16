package edu.uw.zookeeper.safari.storage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

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
            ensemble = EnsembleFromCfg.fromProperties(cfg.get().get());
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

    @Configurable(path="storage", key="ensemble", arg="ensemble", help="address:port,...")
    public static abstract class StorageEnsembleViewConfiguration {
    
        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            Config config = configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path());
            return (config.hasPath(configurable.key())) ? ServerInetAddressView.ensembleFromString(
                        config.getString(configurable.key())) : null;
        }
        
        public static Configuration set(Configuration configuration, EnsembleView<ServerInetAddressView> value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(
                    ConfigFactory.parseMap(
                            ImmutableMap.<String,Object>builder()
                            .put(ConfigUtil.joinPath(configurable.path(), configurable.key()), 
                                    EnsembleView.toString(value)).build()));
        }
        
        public static Configurable getConfigurable() {
            return StorageEnsembleViewConfiguration.class.getAnnotation(Configurable.class);
        }
        
        protected StorageEnsembleViewConfiguration() {}
    }

    public static abstract class EnsembleFromCfg {

        public static EnsembleView<ServerInetAddressView> fromProperties(Properties properties) {
            Set<Map.Entry<Object, Object>> serverProperties = Sets.filter(properties.entrySet(),
                    new Predicate<Map.Entry<Object, Object>>(){
                        @Override
                        public boolean apply(Map.Entry<Object, Object> input) {
                            // TODO Auto-generated method stub
                            return input.getKey().toString().trim().startsWith("server.");
                        }});
            ImmutableSet.Builder<ServerInetAddressView> servers = ImmutableSet.builder();
            for (Map.Entry<Object, Object> property: serverProperties) {
                String[] fields = property.getValue().toString().trim().split(":");
                ServerInetAddressView server;
                try {
                    if (fields.length == 1) {
                        server = ServerInetAddressView.of(
                                InetAddress.getByName("127.0.0.1"), 
                                Integer.parseInt(fields[0]));
                    } else if (fields.length >= 2) {
                        server = ServerInetAddressView.fromString(
                                String.format("%s:%s", fields[0], fields[1]));
                    } else {
                        throw new IllegalArgumentException(String.valueOf(property));
                    }
                    servers.add(server);
                } catch (UnknownHostException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            EnsembleView<ServerInetAddressView> ensemble = EnsembleView.copyOf(servers.build());
            return ensemble;
        }
        
        protected EnsembleFromCfg() {}
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
