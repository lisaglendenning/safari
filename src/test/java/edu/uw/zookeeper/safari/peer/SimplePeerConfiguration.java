package edu.uw.zookeeper.safari.peer;

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
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.peer.PeerAddressView;
import edu.uw.zookeeper.safari.peer.PeerConfiguration;

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
        Identifier peer = ControlZNode.CreateEntity.call(ControlSchema.Safari.Peers.PATH, address, control.materializer()).get();
        TimeValue timeOut = TimeValue.create(0L, TimeUnit.MILLISECONDS);
        return new PeerConfiguration(PeerAddressView.valueOf(peer, address), timeOut);
    }
}
