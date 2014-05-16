package edu.uw.zookeeper.safari.frontend;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.peer.PeerModules;
import edu.uw.zookeeper.safari.region.RegionModules;
import edu.uw.zookeeper.server.ClientAddressConfiguration;

public class FrontendModules {
    
    public static class FrontendClientModule extends Modules.DelegatingComponentProvider {

        protected FrontendClientModule(Injector injector,
                Class<? extends Provider<? extends Component<?>>> delegate) {
            super(injector, delegate);
            // TODO Auto-generated constructor stub
        }
        
    }

    public static class FrontendProvider extends RegionModules.RegionMemberProvider {

        protected final InetSocketAddress address;
        
        @Inject
        public FrontendProvider(
                InetSocketAddress address,
                @Named("storage") List<Component<?>> storage,
                @Named("server") Component<?> server,
                RuntimeModule runtime,
                Injector injector) {
            this(address, storage, server, runtime, injector, PeerModules.PeerProvider.class);
        }

        protected FrontendProvider(
                InetSocketAddress address,
                @Named("storage") List<? extends Component<?>> storage,
                @Named("server") Component<?> server,
                RuntimeModule runtime,
                Injector injector,
                Class<? extends Provider<? extends Component<?>>> delegate) {
            super(storage, server, runtime, injector, delegate);
            this.address = address;
        }
        
        @Override
        public Component<?> get() {
            ClientAddressConfiguration.set(
                    runtime.getConfiguration(), 
                    ServerInetAddressView.of(address));
            return super.get();
        }
    }

    protected FrontendModules() {}
}
