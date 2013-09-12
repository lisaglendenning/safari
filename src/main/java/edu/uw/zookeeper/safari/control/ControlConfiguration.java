package edu.uw.zookeeper.safari.control;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.ClientBuilder;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;

public class ControlConfiguration {
 
    public static com.google.inject.Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public ControlConfiguration getControlConfiguration(Configuration configuration) {
            EnsembleView<ServerInetAddressView> ensemble = 
                    ConfigurableEnsembleView.get(configuration);
            TimeValue timeOut = ConfigurableTimeout.get(configuration);
            return new ControlConfiguration(ensemble, timeOut);
        }
    }

    @Configurable(path="Control", key="Ensemble", arg="control", value="localhost:2381", help="Address:Port,...")
    public static class ConfigurableEnsembleView extends ClientBuilder.ConfigurableEnsembleView {

        public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
            return new ConfigurableEnsembleView().apply(configuration);
        }
    }

    @Configurable(path="Control", key="Timeout", value="30 seconds", help="Time")
    public static class ConfigurableTimeout extends ZooKeeperApplication.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }
    }
    
    protected final EnsembleView<ServerInetAddressView> ensemble;
    protected final TimeValue timeOut;

    public ControlConfiguration(
            EnsembleView<ServerInetAddressView> ensemble,
            TimeValue timeOut) {
        this.ensemble = ensemble;
        this.timeOut = timeOut;
    }
    
    public EnsembleView<ServerInetAddressView> getEnsemble() {
        return ensemble;
    }
    
    public TimeValue getTimeOut() {
        return timeOut;
    }
}
