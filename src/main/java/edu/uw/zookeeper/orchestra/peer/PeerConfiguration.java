package edu.uw.zookeeper.orchestra.peer;

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.control.ControlSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.server.ServerBuilder;

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
            ServerInetAddressView address = ConfigurableServerAddressView.get(configuration);
            ControlSchema.Peers.Entity entityNode = ControlSchema.Peers.Entity.create(address, control.materializer()).get();
            TimeValue timeOut = ConfigurableTimeout.get(configuration);
            PeerConfiguration instance = new PeerConfiguration(PeerAddressView.of(entityNode.get(), address), timeOut);
            LogManager.getLogger(getClass()).info("{}", instance);
            return instance;
        }
    }
    
    @Configurable(arg="peerAddress", path="Peer", key="PeerAddress", value=":2281", help="Address:Port")
    public static class ConfigurableServerAddressView extends ServerBuilder.ConfigurableServerAddressView {

        public static ServerInetAddressView get(Configuration configuration) {
            return new ConfigurableServerAddressView().apply(configuration);
        }
    }

    @Configurable(path="Peer", key="Timeout", value="30 seconds", help="Time")
    public static class ConfigurableTimeout extends ZooKeeperApplication.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }
    }
    
    public static void advertise(Identifier peerId, Materializer<?> materializer) throws KeeperException, InterruptedException, ExecutionException {
        ControlSchema.Peers.Entity entity = ControlSchema.Peers.Entity.of(peerId);
        Operation.ProtocolResponse<?> result = entity.presence().create(materializer).get();
        Operations.unlessError(result.record());
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
