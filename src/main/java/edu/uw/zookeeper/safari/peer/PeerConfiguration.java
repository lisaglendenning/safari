package edu.uw.zookeeper.safari.peer;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.CreateEntity;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class PeerConfiguration extends AbstractModule {

    public static PeerConfiguration create() {
        return new PeerConfiguration();
    }
    
    protected PeerConfiguration() {}
    
    @Override
    protected void configure() {
    }

    @Provides @Peer @Singleton
    public TimeValue getPeerTimeOutConfiguration(
            Configuration configuration) {
        return PeerTimeOutConfiguration.get(configuration);
    }

    @Provides @Peer @Singleton
    public ServerInetAddressView getPeerAddressConfiguration(
            Configuration configuration) {
        return PeerAddressConfiguration.get(configuration);
    }

    @Provides @Peer @Singleton
    public Identifier getPeerIdConfiguration(
            SchemaClientService<ControlZNode<?>,?> control, 
            @Peer ServerInetAddressView address,
            Configuration configuration) throws InterruptedException, ExecutionException, KeeperException {
        if(!control.isRunning()) {
            control.startAsync().awaitRunning();
        }
        Identifier id = CreateEntity.sync(
                address, 
                ControlSchema.Safari.Peers.class,
                control.materializer()).get();
        LogManager.getLogger(Peer.class).info("Peer at {} is {}", address, id);
        return id;
    }
    
    @Configurable(arg="peerAddress", path="peer", value=":2281", help="address:port")
    public static abstract class PeerAddressConfiguration {

        public static Configurable getConfigurable() {
            return PeerAddressConfiguration.class.getAnnotation(Configurable.class);
        }
        
        public static ServerInetAddressView get(Configuration configuration) {
            Configurable configurable = getConfigurable();
            String value = 
                    configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                        .getString(configurable.arg());
            try {
                return ServerInetAddressView.fromString(value);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(value, e);
            }
        }
        
        public static Configuration set(Configuration configuration, ServerInetAddressView value) {
            Configurable configurable = getConfigurable();
            return configuration.withConfig(
                    ConfigFactory.parseMap(
                            ImmutableMap.<String,Object>builder()
                            .put(ConfigUtil.joinPath(configurable.path(), configurable.arg()), value.toString())
                            .build()));
        }
        
        protected PeerAddressConfiguration() {}
    }

    @Configurable(path="peer", key="timeout", value="30 seconds", help="time")
    public static abstract class PeerTimeOutConfiguration {

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
            return PeerTimeOutConfiguration.class.getAnnotation(Configurable.class);
        }
        
        protected PeerTimeOutConfiguration() {}
    }
}
