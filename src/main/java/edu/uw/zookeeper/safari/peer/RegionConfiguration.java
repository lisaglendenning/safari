package edu.uw.zookeeper.safari.peer;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.backend.BackendConfiguration;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;

public class RegionConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public RegionConfiguration getEnsembleConfiguration(
                BackendConfiguration backendConfiguration,
                ControlMaterializerService control) throws InterruptedException, ExecutionException, KeeperException {
            checkState(control.isRunning());
            EnsembleView<ServerInetAddressView> myView = backendConfiguration.getView().getEnsemble();
            Identifier regionId = ControlZNode.CreateEntity.call(
                    ControlSchema.Safari.Regions.PATH,
                    myView, 
                    control.materializer()).get();
            RegionConfiguration instance = new RegionConfiguration(regionId);
            LogManager.getLogger(RegionConfiguration.class).info("{}", instance);
            return instance;
        }
    }
    
    private final Identifier region;

    public RegionConfiguration(Identifier region) {
        this.region = region;
    }
    
    public Identifier getRegion() {
        return region;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("region", region).toString();
    }
}
