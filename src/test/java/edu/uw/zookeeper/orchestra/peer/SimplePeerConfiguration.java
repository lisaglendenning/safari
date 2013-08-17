package edu.uw.zookeeper.orchestra.peer;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.net.intravm.IntraVmFactory;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;

public class SimplePeerConfiguration extends AbstractModule {

    public static SimplePeerConfiguration create() {
        return new SimplePeerConfiguration();
    }
    
    @Override
    protected void configure() {
    }

    @Provides @Singleton
    public PeerConfiguration getPeerConfiguration(
            ControlMaterializerService<?> controlClient, 
            IntraVmFactory net) throws InterruptedException, ExecutionException, KeeperException {
        ServerInetAddressView conductorAddress = 
                ServerInetAddressView.of((InetSocketAddress) net.addresses().get());
        ControlSchema.Peers.Entity entityNode = ControlSchema.Peers.Entity.create(conductorAddress, controlClient.materializer()).get();
        return new PeerConfiguration(PeerAddressView.of(entityNode.get(), conductorAddress));
    }
}
