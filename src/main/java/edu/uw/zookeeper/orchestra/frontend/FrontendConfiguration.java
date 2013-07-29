package edu.uw.zookeeper.orchestra.frontend;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.util.Configuration;

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
            ServerInetAddressView view = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance().get(configuration);
            return new FrontendConfiguration(view);
        }
    }
    
    public static void advertise(Identifier peerId, ServerInetAddressView address, Materializer<?,?> materializer) throws InterruptedException, ExecutionException, KeeperException {
        Orchestra.Peers.Entity entityNode = Orchestra.Peers.Entity.of(peerId);
        Orchestra.Peers.Entity.ClientAddress clientAddressNode = Orchestra.Peers.Entity.ClientAddress.create(address, entityNode, materializer).get();
        if (! address.equals(clientAddressNode.get())) {
            throw new IllegalStateException(clientAddressNode.get().toString());
        }        
    }

    protected final ServerInetAddressView address;

    public FrontendConfiguration(ServerInetAddressView address) {
        this.address = address;
    }    
    
    public ServerInetAddressView getAddress() {
        return address;
    }
}
