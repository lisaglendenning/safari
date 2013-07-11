package edu.uw.zookeeper.orchestra.peer;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.server.ServerApplicationModule;
import edu.uw.zookeeper.util.Pair;

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
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            Materializer<?,?> materializer = controlClient.materializer();
            ServerInetAddressView conductorAddress = ServerApplicationModule.ConfigurableServerAddressViewFactory.newInstance(
                            "Peer", "address", "peerAddress", "", 2281).get(runtime.configuration());
            Orchestra.Peers.Entity entityNode = Orchestra.Peers.Entity.create(conductorAddress, materializer);
            return new PeerConfiguration(PeerAddressView.of(entityNode.get(), conductorAddress));
        }
    }

    public static void advertise(Identifier peerId, Materializer<?,?> materializer) throws KeeperException, InterruptedException, ExecutionException {
        Orchestra.Peers.Entity entityNode = Orchestra.Peers.Entity.of(peerId);
        Orchestra.Peers.Entity.Presence presenceNode = Orchestra.Peers.Entity.Presence.of(entityNode);
        Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = materializer.operator().create(presenceNode.path()).submit().get();
        Operations.unlessError(result.second().response(), result.toString());
    }
    
    private final PeerAddressView address;
    
    public PeerConfiguration(PeerAddressView address) {
        this.address = address;
    }
    
    public PeerAddressView getView() {
        return address;
    }
}
