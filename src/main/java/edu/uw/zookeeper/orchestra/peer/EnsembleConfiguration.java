package edu.uw.zookeeper.orchestra.peer;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.backend.BackendConfiguration;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;

public class EnsembleConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public EnsembleConfiguration getEnsembleConfiguration(
                BackendConfiguration backendConfiguration,
                ControlClientService<?> controlClient) throws InterruptedException, ExecutionException, KeeperException {
            // Find my ensemble
            EnsembleView<ServerInetAddressView> myView = backendConfiguration.getView().getEnsemble();
            Orchestra.Ensembles.Entity ensembleNode = Orchestra.Ensembles.Entity.create(myView, controlClient.materializer());
            return new EnsembleConfiguration(ensembleNode.get());
        }
    }
    
    private final Identifier ensemble;

    public EnsembleConfiguration(Identifier ensemble) {
        this.ensemble = ensemble;
    }
    
    public Identifier getEnsemble() {
        return ensemble;
    }
}
