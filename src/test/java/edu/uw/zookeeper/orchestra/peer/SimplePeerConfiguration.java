package edu.uw.zookeeper.orchestra.peer;

import static com.google.common.base.Preconditions.checkState;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.TimeValue;
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
            ControlMaterializerService control, 
            Factory<? extends SocketAddress> addresses) throws InterruptedException, ExecutionException, KeeperException {
        checkState(control.isRunning());
        ServerInetAddressView address = 
                ServerInetAddressView.of((InetSocketAddress) addresses.get());
        ControlSchema.Peers.Entity entityNode = ControlSchema.Peers.Entity.create(address, control.materializer()).get();
        TimeValue timeOut = TimeValue.create(0L, TimeUnit.MILLISECONDS);
        return new PeerConfiguration(PeerAddressView.of(entityNode.get(), address), timeOut);
    }
}
