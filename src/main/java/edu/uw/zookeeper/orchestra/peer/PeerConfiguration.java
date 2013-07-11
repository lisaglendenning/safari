package edu.uw.zookeeper.orchestra.peer;

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

public class PeerConfiguration {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public PeerConfiguration getPeerConfiguration(
                ControlClientService<?> controlClient, 
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            Materializer<?,?> materializer = controlClient.materializer();
            ServerInetAddressView conductorAddress = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance(
                            "Peer", "address", "peerAddress", "", 2281).get(runtime.configuration());
            Orchestra.Peers.Entity entityNode = Orchestra.Peers.Entity.create(conductorAddress, materializer);
            return new PeerConfiguration(PeerAddressView.of(entityNode.get(), conductorAddress));
        }
    }

    private final PeerAddressView address;
    
    public PeerConfiguration(PeerAddressView address) {
        this.address = address;
    }
    
    public PeerAddressView getView() {
        return address;
    }
}
