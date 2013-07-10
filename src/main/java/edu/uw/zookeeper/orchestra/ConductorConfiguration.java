package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.server.ServerApplicationModule;

public class ConductorConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public ConductorConfiguration getConductorConfiguration(
                ControlClientService<?> controlClient, 
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            Materializer<?,?> materializer = controlClient.materializer();
            ServerInetAddressView conductorAddress = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance(
                            "Conductor", "address", "conductorAddress", "", 2281).get(runtime.configuration());
            Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.create(conductorAddress, materializer);
            return new ConductorConfiguration(ConductorAddressView.of(entityNode.get(), conductorAddress));
        }
    }

    private final ConductorAddressView address;
    
    public ConductorConfiguration(ConductorAddressView address) {
        this.address = address;
    }
    
    public ConductorAddressView getAddress() {
        return address;
    }
}
