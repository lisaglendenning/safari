package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.TimeoutFactory;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.server.ServerApplicationModule;

public class FrontendConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public FrontendConfiguration getFrontendConfiguration(Configuration configuration) {
            ServerInetAddressView address = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(configuration);
            TimeValue timeOut = TimeoutFactory.newInstance(CONFIG_PATH).get(configuration);
            return new FrontendConfiguration(address, timeOut);
        }
    }
    
    public static void advertise(Identifier peerId, ServerInetAddressView address, Materializer<?> materializer) throws InterruptedException, ExecutionException, KeeperException {
        ControlSchema.Peers.Entity entityNode = ControlSchema.Peers.Entity.of(peerId);
        ControlSchema.Peers.Entity.ClientAddress valueNode = ControlSchema.Peers.Entity.ClientAddress.create(address, entityNode, materializer).get();
        if (! address.equals(valueNode.get())) {
            throw new IllegalStateException(String.valueOf(valueNode.get()));
        }        
    }

    public static final String CONFIG_PATH = "Frontend";
    
    private final ServerInetAddressView address;
    private final TimeValue timeOut;

    public FrontendConfiguration(
            ServerInetAddressView address,
            TimeValue timeOut) {
        this.address = address;
        this.timeOut = timeOut;
    }    
    
    public ServerInetAddressView getAddress() {
        return address;
    }
    
    public TimeValue getTimeOut() {
        return timeOut;
    }
}
