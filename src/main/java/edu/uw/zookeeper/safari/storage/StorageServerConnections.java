package edu.uw.zookeeper.safari.storage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.Cfg;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.client.ZxidTracker;

public class StorageServerConnections extends AbstractModule {

    public static StorageServerConnections create() {
        return new StorageServerConnections();
    }
    
    protected StorageServerConnections() {}
    
    @Override
    protected void configure() {
        bind(Key.get(ZxidReference.class, Storage.class)).to(Key.get(ZxidTracker.class, Storage.class));
    }

    @Provides @Storage @Singleton
    public ServerInetAddressView getStorageServerConfiguration(
            StorageCfgFile cfg,
            Configuration configuration) {
        ServerInetAddressView address;
        if (cfg.get().isPresent()) {
            address = StorageServerAddressConfiguration.fromProperties(cfg.get().get());
        } else {
            address = StorageServerAddressConfiguration.get(configuration);
        }
        LogManager.getLogger(getClass()).info("Storage server is {}", address);
        return address;
    }
    
    @Provides @Storage @Singleton
    public ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> getStorageServerClientFactory(
            @Storage ServerInetAddressView server,
            @Storage EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> factory,
            ScheduledExecutorService scheduler) {
        return factory.get(server);
    }

    @Provides @Storage @Singleton
    public ZxidTracker getStorageServerZxids(
            @Storage ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> factory) {
        return factory.second();
    }
    
    @Provides @Storage
    public ListenableFuture<? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> newStorageServerClient(
            @Storage ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
            @Storage ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> factory) {
        Services.startAndWait(connections);
        return factory.get();
    }

    @Configurable(path="storage", key="server", arg="storage", help="address:port")
    public static abstract class StorageServerAddressConfiguration {

        public static Configurable getConfigurable() {
            return StorageServerAddressConfiguration.class.getAnnotation(Configurable.class);
        }

        public static ServerInetAddressView fromProperties(Properties properties) {
            ServerInetAddressView server;
            int clientPort = Integer.parseInt(Cfg.Key.CLIENT_PORT.getValue(properties));
            String clientAddress = Cfg.Key.CLIENT_PORT_ADDRESS.getValue(properties);
            try {
                if (clientAddress == null) {
                    server = ServerInetAddressView.of(
                            InetAddress.getByName("127.0.0.1"), 
                            clientPort);
                } else {
                    server = ServerInetAddressView.fromString(
                            String.format("%s:%d", clientAddress, 
                                    clientPort));
                }
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(e);
            }
            return server;
        }
        
        public static ServerInetAddressView get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            String value = 
                    configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                        .getString(configurable.key());
            try {
                return ServerInetAddressView.fromString(value);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(value, e);
            }
        }
        
        public static Configuration set(Configuration configuration, ServerInetAddressView value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(ConfigFactory.parseMap(ImmutableMap.<String,Object>builder().put(ConfigUtil.joinPath(configurable.path(), configurable.key()), value.toString()).build()));
        }
        
        protected StorageServerAddressConfiguration() {}
    }
}
