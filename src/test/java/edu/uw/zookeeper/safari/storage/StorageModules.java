package edu.uw.zookeeper.safari.storage;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.Modules.SafariServerModuleProvider;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;
import edu.uw.zookeeper.server.SimpleServerBuilder;

public class StorageModules {

    public static @Named("storage") Component<?> newStorageSingletonEnsemble(
            Component<?> parent) {
        return newStorageSingletonEnsemble(parent, Names.named("storage"));
    }

    public static Component<?> newStorageSingletonEnsemble(
            Component<?> parent, Named name) {
        return Modules.newServerComponent(parent, name);
    }
    
    public static List<Component<?>> newStorageEnsemble(Component<?> parent, int size) {
        return newStorageEnsemble(parent, size, "storage-%d");
    }
    
    public static List<Component<?>> newStorageEnsemble(Component<?> parent, int size, String format) {
        ImmutableList.Builder<Component<?>> servers = ImmutableList.builder();
        for (int i=0; i<size; ++i) {
            Named name = Names.named(String.format(format, i+1));
            Component<?> component = Modules.newServerComponent(parent, name);
            servers.add(component);
        }
        return servers.build();
    }

    public static @Named("client") Component<?> newStorageSingletonClient(
            final Component<?> storage,
            final Iterable<? extends Component<?>> components) {
        return newStorageClient(
                ImmutableList.<Component<?>>of(storage), 0, components, Names.named("client"));
    }

    public static Component<?> newStorageClient(
            final List<Component<?>> storage,
            final int server,
            final Iterable<? extends Component<?>> components,
            final Named name) {
        return newStorageClient(
                storage, server, components, name, 
                ImmutableList.<com.google.inject.Module>of(),
                StorageClientProvider.class,
                ImmutableList.<SafariModule>of(),
                StorageModuleProvider.class);
    }

    public static Component<?> newStorageClient(
            final List<Component<?>> storage,
            final int server,
            final Iterable<? extends Component<?>> components,
            final Named name,
            final Iterable<? extends com.google.inject.Module> modules,
            final Class<? extends Provider<? extends Component<?>>> provider,
            final Iterable<? extends SafariModule> safariModules,
            final Class<? extends Provider<List<com.google.inject.Module>>> module) {
        return Modules.newSafariComponent(
                Iterables.concat(components, storage), 
                name, 
                ImmutableList.<com.google.inject.Module>builder()
                    .addAll(modules)
                    .add(StorageServerModule.create(storage, server))
                    .add(SafariServerModuleProvider.module(
                            ImmutableList.<SafariModule>builder()
                                .add(Module.create())
                                .addAll(safariModules)
                                .build()))
                    .build(),
                provider, 
                module);
     }
    
    public static class StorageServerModule extends AbstractModule {

        public static StorageServerModule create(
                List<Component<?>> storage,
                int server) {
            return new StorageServerModule(storage, server);
        }
        
        protected final List<Component<?>> storage;
        protected final int server;
        
        protected StorageServerModule(
                List<Component<?>> storage,
                int server) {
            this.storage = storage;
            this.server = server;
        }
        
        @Override
        protected void configure() {
            bind(new TypeLiteral<List<Component<?>>>(){}).annotatedWith(Names.named("storage")).toInstance(storage);
            bind(new TypeLiteral<Component<?>>(){}).annotatedWith(Names.named("server")).toInstance(storage.get(server));
        }
    }
    
    public static class StorageClientProvider extends Modules.DelegatingComponentProvider {

        protected final @Named("storage") List<? extends Component<?>> storage;
        protected final @Named("server") Component<?> server;
        protected final RuntimeModule runtime;

        @Inject
        public StorageClientProvider(
                @Named("storage") List<Component<?>> storage,
                @Named("server") Component<?> server,
                RuntimeModule runtime,
                Injector injector) {
            this(storage, server, runtime, injector, Modules.ServiceComponentProvider.class);
        }
        
        protected StorageClientProvider(
                @Named("storage") List<? extends Component<?>> storage,
                @Named("server") Component<?> server,
                RuntimeModule runtime,
                Injector injector,
                Class<? extends Provider<? extends Component<?>>> delegate) {
            super(injector, delegate);
            this.storage = storage;
            this.server = server;
            this.runtime = runtime;
        }
        
        @Override
        public Component<?> get() {
            Services.startAndWait(server.injector().getInstance(Service.class));
            final ImmutableList.Builder<ServerInetAddressView> addresses = ImmutableList.builder();
            for (Component<?> server: storage) {
                addresses.add(server.injector().getInstance(SimpleServerBuilder.class).getConnectionsBuilder().getConnectionBuilder().getAddress());
            }
            final EnsembleView<ServerInetAddressView> ensemble = EnsembleView.copyOf(addresses.build());
            StorageEnsembleConnections.StorageEnsembleViewConfiguration.set(
                    runtime.getConfiguration(), 
                    ensemble);
            final ServerInetAddressView address = server.injector().getInstance(SimpleServerBuilder.class).getConnectionsBuilder().getConnectionBuilder().getAddress();
            StorageServerConnections.StorageServerAddressConfiguration.set(
                    runtime.getConfiguration(), 
                    address);
            return super.get();
        }
    }

    public static class StorageModuleProvider extends Modules.SafariServerModuleProvider {

        @Inject
        public StorageModuleProvider(
                RuntimeModule runtime,
                List<SafariModule> safariModules) {
            super(runtime, safariModules);
        }
        
        @Override
        public List<com.google.inject.Module> get() {
            return ImmutableList.<com.google.inject.Module>builder()
                    .add(JacksonModule.create())
                    .addAll(super.get()).build();
        }
    }
    
    protected StorageModules() {}
}
