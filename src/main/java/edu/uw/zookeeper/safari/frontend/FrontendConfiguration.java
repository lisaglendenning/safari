package edu.uw.zookeeper.safari.frontend;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.CreateOrEquals;
import edu.uw.zookeeper.server.ConfigurableServerAddressView;

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
            ServerInetAddressView address = ConfigurableServerAddressView.get(configuration);
            TimeValue timeOut = ConfigurableTimeout.get(configuration);
            return new FrontendConfiguration(address, timeOut);
        }
    }
    
    public static ListenableFuture<Optional<ServerInetAddressView>> advertise(
            final Identifier peer, 
            final ServerInetAddressView value, 
            final Materializer<ControlZNode<?>,?> materializer) {    
        ZNodePath path = ControlSchema.Safari.Peers.Peer.ClientAddress.pathOf(peer);
        return Futures.transform(CreateOrEquals.create(path, value, materializer), 
                new Function<Optional<ServerInetAddressView>, Optional<ServerInetAddressView>>() {
                    @Override
                    public Optional<ServerInetAddressView> apply(
                            Optional<ServerInetAddressView> input) {
                        if (input.isPresent()) {
                            throw new IllegalStateException(String.format("%s != %s", value, input.get()));
                        }
                        return input;
                    }
        });
    }

    @Configurable(path="frontend", key="timeout", value="30 seconds", help="time")
    public static class ConfigurableTimeout extends ZooKeeperApplication.ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }
    }
    
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
