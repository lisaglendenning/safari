package edu.uw.zookeeper.safari.backend;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Objects;
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
            ServerInetAddressView clientAddress = ConfigurableAddressView.get(configuration);
            if (clientAddress == null) {
                clientAddress = BackendAddressDiscovery.get();
            }
            EnsembleView<ServerInetAddressView> ensemble = ConfigurableEnsembleView.get(configuration);
            if (ensemble == null) {
                ensemble = BackendEnsembleViewFactory.get();
            }
            TimeValue timeOut = ConfigurableTimeout.get(configuration);
            BackendConfiguration instance = new BackendConfiguration(BackendView.of(clientAddress, ensemble), timeOut);
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
    
    @Configurable(path="Backend", key="Timeout", value="30 seconds", help="Time")
    public static class ConfigurableTimeout extends ZooKeeperApplication.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }
    }
    
    @Configurable(path="Backend", key="ClientAddress", arg="backend", help="Address:Port")
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

    @Configurable(path="Backend", key="Ensemble", arg="ensemble", help="Address:Port,...")
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
}
