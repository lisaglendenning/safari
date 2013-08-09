package edu.uw.zookeeper.orchestra.peer;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.protocol.Operation;
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
                ControlMaterializerService<?> controlClient, 
                Configuration configuration) throws InterruptedException, ExecutionException, KeeperException {
            ServerInetAddressView conductorAddress = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance(
                            "Peer", "address", "peerAddress", "", 2281).get(configuration);
            ControlSchema.Peers.Entity entityNode = ControlSchema.Peers.Entity.create(conductorAddress, controlClient.materializer()).get();
            return new PeerConfiguration(PeerAddressView.of(entityNode.get(), conductorAddress));
        }
    }

    public static void advertise(Identifier peerId, Materializer<?> materializer) throws KeeperException, InterruptedException, ExecutionException {
        ControlSchema.Peers.Entity entity = ControlSchema.Peers.Entity.of(peerId);
        Operation.ProtocolResponse<?> result = entity.presence().create(materializer).get();
        Operations.unlessError(result.getRecord());
    }
    
    private final PeerAddressView address;
    
    public PeerConfiguration(PeerAddressView address) {
        this.address = address;
    }
    
    public PeerAddressView getView() {
        return address;
    }
}
