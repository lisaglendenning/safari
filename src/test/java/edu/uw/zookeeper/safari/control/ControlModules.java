package edu.uw.zookeeper.safari.control;


import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public abstract class ControlModules {

    public static @Named("control") Component<?> newControlSingletonEnsemble(
            Component<?> parent) {
        final Named name = Names.named("control");
        return Modules.newServerComponent(parent, name);
    }

    public static @Named("client") Component<?> newControlClient(
            final Iterable<? extends Component<?>> components) {
        return newControlClient(
                components, 
                ImmutableList.<SafariModule>of(), 
                ControlModules.ControlClientProvider.class);
    }

    public static @Named("client") Component<?> newControlClient(
            final Iterable<? extends Component<?>> components,
            final Iterable<? extends SafariModule> modules,
            final Class<? extends Provider<? extends Component<?>>> provider) {
        final Named name = Names.named("client");
        return newControlClient(components, name, modules, provider);
    }

    public static Component<?> newControlClient(
            final Iterable<? extends Component<?>> components,
            final Named name,
            final Iterable<? extends SafariModule> modules,
            final Class<? extends Provider<? extends Component<?>>> provider) {
        return Modules.newSafariComponent(
                components, 
                name,
                ImmutableList.<SafariModule>builder()
                    .add(Module.create())
                    .addAll(modules)
                    .build(),
                provider);
    }
    
    public static class ControlClientProvider extends Modules.DelegatingComponentProvider {

        protected final @Named("control") Component<?> control;
        protected final RuntimeModule runtime;

        @Inject
        public ControlClientProvider(
                @Named("control") Component<?> control,
                RuntimeModule runtime,
                Injector injector) {
            this(control, runtime, injector, Modules.ServiceComponentProvider.class);
        }
        
        protected ControlClientProvider(
                @Named("control") Component<?> control,
                RuntimeModule runtime,
                Injector injector,
                Class<? extends Provider<? extends Component<?>>> delegate) {
            this(control, runtime, injector.getInstance(delegate));
        }
        
        protected ControlClientProvider(
                @Named("control") Component<?> control,
                RuntimeModule runtime,
                Provider<? extends Component<?>> delegate) {
            super(delegate);
            this.runtime = runtime;
            this.control = control;
        }
        
        @Override
        public Component<?> get() {
            final EnsembleView<ServerInetAddressView> ensemble = EnsembleView.copyOf(control.injector().getInstance(SimpleServerBuilder.class).getConnectionsBuilder().getConnectionBuilder().getAddress());
            ControlEnsembleConnections.ControlEnsembleViewConfiguration.set(
                    runtime.getConfiguration(), 
                    ensemble);
            return super.get();
        }
    }
    
    protected ControlModules() {}
}
