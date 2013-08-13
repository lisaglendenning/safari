package edu.uw.zookeeper.orchestra.control;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ClientApplicationModule;
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
                    ClientApplicationModule.ConfigurableEnsembleViewFactory.newInstance(
                            CONFIG_PATH, ENSEMBLE_CONFIG_KEY, ENSEMBLE_ARG, DEFAULT_ENSEMBLE_ADDRESS, DEFAULT_ENSEMBLE_PORT)
                        .get(configuration);
            TimeValue timeOut = ClientApplicationModule.TimeoutFactory.newInstance(CONFIG_PATH)
                        .get(configuration);
            return new ControlConfiguration(ensemble, timeOut);
        }
    }

    public static final String CONFIG_PATH = "Control";
    public static final String ENSEMBLE_ARG = "control";
    public static final String ENSEMBLE_CONFIG_KEY = "Ensemble";
    public static final String DEFAULT_ENSEMBLE_ADDRESS = "localhost";
    public static final int DEFAULT_ENSEMBLE_PORT = 2381;
    
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
