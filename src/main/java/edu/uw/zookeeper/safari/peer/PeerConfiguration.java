package edu.uw.zookeeper.safari.peer;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.server.ConfigurableServerAddressView;

public class PeerConfiguration {

    public static com.google.inject.Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public PeerConfiguration getPeerConfiguration(
                ControlMaterializerService control, 
                Configuration configuration) throws InterruptedException, ExecutionException, KeeperException {
            checkState(control.isRunning());
            ServerInetAddressView address = PeerConfigurableServerAddressView.get(configuration);
            Identifier peerId = ControlZNode.CreateEntity.call(
                    ControlSchema.Safari.Peers.PATH,
                    address, 
                    control.materializer()).get();
            TimeValue timeOut = ConfigurableTimeout.get(configuration);
            PeerConfiguration instance = new PeerConfiguration(PeerAddressView.valueOf(peerId, address), timeOut);
            LogManager.getLogger(getClass()).info("{}", instance);
            return instance;
        }
    }
    
    @Configurable(arg="peerAddress", path="peer", key="peerAddress", value=":2281", help="address:port")
    public static class PeerConfigurableServerAddressView extends ConfigurableServerAddressView {

        public static ServerInetAddressView get(Configuration configuration) {
            return new PeerConfigurableServerAddressView().apply(configuration);
        }
    }

    @Configurable(path="peer", key="timeout", value="30 seconds", help="time")
    public static class ConfigurableTimeout extends ZooKeeperApplication.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }
    }
    
    public static ListenableFuture<Void> advertise(
            final Identifier peer, 
            final Materializer<ControlZNode<?>,?> materializer) {
        ZNodePath path = ControlSchema.Safari.Peers.PATH.join(ZNodeLabel.fromString(peer.toString())).join(ControlSchema.Safari.Peers.Peer.Presence.LABEL);
        return Futures.transform(materializer.create(path).call(), 
                new Function<Operation.ProtocolResponse<?>, Void>() {
                    @Override
                    public Void apply(
                            Operation.ProtocolResponse<?> input) {
                        try {
                            Operations.unlessError(input.record());
                        } catch (KeeperException e) {
                            throw new IllegalStateException(String.format("Error creating presence for %s", peer), e);
                        }
                        return null;
                    }
        });
    }

    public static final String CONFIG_PATH = "Peer";
    
    private final PeerAddressView view;
    private final TimeValue timeOut;
    
    public PeerConfiguration(
            PeerAddressView view,
            TimeValue timeOut) {
        this.view = view;
        this.timeOut = timeOut;
    }
    
    public PeerAddressView getView() {
        return view;
    }
    
    public TimeValue getTimeOut() {
        return timeOut;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("view", view).add("timeOut", timeOut).toString();
    }
}
