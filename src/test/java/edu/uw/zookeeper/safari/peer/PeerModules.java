package edu.uw.zookeeper.safari.peer;


import java.net.InetSocketAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.ControlModules;

public class PeerModules {

    public static @Named("peer") Component<?> newPeer(
            final Iterable<? extends Component<?>> components) {
        return newPeer(
                components, 
                PeerProvider.class, 
                ImmutableList.<SafariModule>of());
    }

    public static @Named("peer") Component<?> newPeer(
            final Iterable<? extends Component<?>> components,
            final Class<? extends Provider<? extends Component<?>>> provider,
            final Iterable<? extends SafariModule> modules) {
        return newPeer(components, Names.named("peer"), provider, modules);
    }

    public static Component<?> newPeer(
            final Iterable<? extends Component<?>> components,
            final Named name,
            final Class<? extends Provider<? extends Component<?>>> provider,
            final Iterable<? extends SafariModule> modules) {
        return ControlModules.newControlClient(
                components, name, Iterables.concat(ImmutableList.of(Module.create()), modules), provider);
    }

    public static class PeerProvider extends ControlModules.ControlClientProvider {

        protected final InetSocketAddress address;
        
        @Inject
        public PeerProvider(
                InetSocketAddress address,
                @Named("control") Component<?> control,
                RuntimeModule runtime,
                Injector injector) {
            this(address, control, runtime, injector, Modules.ServiceComponentProvider.class);
        }
        
        protected PeerProvider(
                InetSocketAddress address,
                @Named("control") Component<?> control,
                RuntimeModule runtime,
                Injector injector,
                Class<? extends Provider<? extends Component<?>>> delegate) {
            super(control, runtime, injector, delegate);
            this.address = address;
        }
        
        @Override
        public Component<?> get() {
            Services.startAndWait(control.injector().getInstance(Service.class));
            PeerConfiguration.PeerAddressConfiguration.set(
                    runtime.getConfiguration(), 
                    ServerInetAddressView.of(address));
            return super.get();
        }
    }
    
    protected PeerModules() {}
}
