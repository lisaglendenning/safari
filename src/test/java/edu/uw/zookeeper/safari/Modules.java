package edu.uw.zookeeper.safari;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.DefaultRuntimeModule.SingleThreadScheduledExectorFactory;
import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.SimpleClientProvider;
import edu.uw.zookeeper.client.SimpleEnsembleClientProvider;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.GuiceRuntimeModule;
import edu.uw.zookeeper.common.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.net.AnonymousClientModule;
import edu.uw.zookeeper.net.IntraVmAsNetModule;
import edu.uw.zookeeper.server.SimpleServerBuilder;
import edu.uw.zookeeper.server.SimpleServerModule;

public abstract class Modules {

    public static Component<Named> newRootComponent() {
        return Component.create(
                Names.named("root"), 
                Guice.createInjector(IntraVmAsNetModule.create()));
    }

    public static Component<?> newNamedComponent(
            Iterable<? extends Component<?>> components,
                    Named name,
                    Iterable<? extends com.google.inject.Module> modules) {
        modules = ImmutableList.<com.google.inject.Module>builder()
                .add(Modules.AnnotatedComponentsModule.forComponents(components))
                .add(Modules.NamedModule.create(name))
                .add(Modules.RuntimeModuleProvider.module())
                .addAll(modules)
                .build();
        Component<?> root = null;
        for (Component<?> component: components) {
            if (component.annotation().equals(Names.named("root"))) {
                root = component;
                break;
            }
        }
        return ((root == null) ? 
                Guice.createInjector(modules) : 
                    root.injector().createChildInjector(modules))
                    .getInstance(Key.get(new TypeLiteral<Component<?>>(){}));
    }

    public static Component<?> newSafariComponent(
            final Iterable<? extends Component<?>> components,
                    final Named name,
                    final List<SafariModule> safariModules,
                    final Class<? extends Provider<? extends Component<?>>> provider) {
        return newSafariComponent(
                components, 
                name,
                ImmutableList.of(SafariServerModuleProvider.module(safariModules)), 
                provider, 
                SafariServerModuleProvider.class);
    }

    public static Component<?> newSafariComponent(
            final Iterable<? extends Component<?>> components,
            final Named name,
            final Iterable<? extends com.google.inject.Module> modules,
            final Class<? extends Provider<? extends Component<?>>> provider,
            final Class<? extends Provider<List<com.google.inject.Module>>> module) {
        return Modules.newNamedComponent(
                components,
                name,
                ImmutableList.<com.google.inject.Module>builder()
                .addAll(modules)
                .add(Modules.ServiceComponentProvider.module(
                        provider, module))
                        .build());
    }

    public static ImmutableList<Component<?>> newServerAndClientComponents() {
        final Component<?> root = Modules.newRootComponent();
        return ImmutableList.<Component<?>>builder().add(root).addAll(newServerAndClientComponents(root)).build();
    }

    public static ImmutableList<Component<?>> newServerAndClientComponents(
            final Component<?> parent) {
        final Component<?> server = Modules.newServerComponent(parent, Names.named("server"));
        final Component<?> client = Modules.newClientComponent(parent, server, Names.named("client"));
        return ImmutableList.of(server, client);
    }

    public static Component<?> newServerComponent(
            Component<?> parent, Named name) {
        return Modules.newNamedComponent(
                ImmutableList.of(parent), 
                name,
                ImmutableList.of(
                        Modules.ServiceComponentProvider.module(
                                ServerModulesProvider.class)));
    }
    
    public static Component<?> newClientComponent(
            Component<?> parent, Component<?> server, Named name) {
        final EnsembleView<ServerInetAddressView> ensemble = EnsembleView.copyOf(server.injector().getInstance(SimpleServerBuilder.class).getConnectionsBuilder().getConnectionBuilder().getAddress());
        return newClientComponent(parent, ensemble, name);
    }
    
    public static Component<?> newClientComponent(
            Component<?> parent, final EnsembleView<ServerInetAddressView> ensemble, Named name) {
        return Modules.newNamedComponent(
                ImmutableList.of(
                        parent.createChildComponent(parent.annotation(),
                        ImmutableList.of(
                                new AbstractModule() {
                                    @Override
                                    protected void configure() {
                                        bind(new TypeLiteral<EnsembleView<ServerInetAddressView>>(){}).toInstance(ensemble);
                                    }
                                }))), 
                name,
                ImmutableList.of(
                        Modules.ServiceComponentProvider.module(
                                ClientModulesProvider.class)));
    }
    
    public static class ServiceModule extends AbstractModule {
    
        public static ServiceModule create(Function<Injector, ? extends Service> getService) {
            return new ServiceModule(getService);
        }

        public static <T extends Service> ServiceModule forType(Class<T> type) {
            return create(getInstance(type));
        }

        public static <T extends Service> ServiceModule instantiates(Key<?> key, Class<T> type) {
            return create(instantiating(key, getInstance(type)));
        }

        public static <T> Function<Injector, T> getInstance(final Class<T> type) {
            return new Function<Injector, T>() {
                @Override
                public T apply(Injector injector) {
                    return injector.getInstance(type);
                }
            };
        }
        
        public static <T> Function<Injector, T> instantiating(final Key<?> key, final Function<Injector, T> delegate) {
            return new Function<Injector, T>() {
                @Override
                public T apply(Injector injector) {
                    injector.getInstance(key);
                    return delegate.apply(injector);
                }
            };
        }
        
        protected final Function<Injector, ? extends Service> getService;
        
        protected ServiceModule(Function<Injector, ? extends Service> getService) {
            this.getService = getService;
        }
        
        @Provides @Singleton
        public Service getService(Injector injector) {
            return getService.apply(injector);
        }
        
        @Override
        protected void configure() {
        }
    }

    public static class ServiceMonitorProvider implements Provider<ServiceMonitor> {

        protected final boolean stopOnTerminate;
        protected final List<Service> services;
        
        protected ServiceMonitorProvider(
                boolean stopOnTerminate,
                List<Service> services) {
            this.stopOnTerminate = stopOnTerminate;
            this.services = services;
        }

        @Override
        public ServiceMonitor get() {
            return ServiceMonitor.create(
                    Optional.<Executor>absent(), 
                    MoreExecutors.directExecutor(), 
                    stopOnTerminate,
                    services);
        }
    }

    public static class StoppingServiceMonitorProvider extends ServiceMonitorProvider {
        @Inject
        protected StoppingServiceMonitorProvider(
                List<Service> services) {
            super(true, services);
        }
    }

    public static class NonStoppingServiceMonitorProvider extends ServiceMonitorProvider {
        @Inject
        protected NonStoppingServiceMonitorProvider(
                List<Service> services) {
            super(false, services);
        }
    }
    
    public static class ComponentServicesProvider implements Provider<List<Service>> {
    
        protected final List<Component<?>> components;
        
        @Inject
        protected ComponentServicesProvider(
                List<Component<?>> components) {
            this.components = components;
        }
    
        @Override
        public List<Service> get() {
            ImmutableList.Builder<Service> services = ImmutableList.builder();
            for (Component<?> component: components) {
                Binding<Service> binding = component.injector().getExistingBinding(Key.get(Service.class));
                if (binding != null) {
                    services.add(binding.getProvider().get());
                }
            }
            return services.build();
        }
    }

    public static class ComponentsModule extends AbstractModule {

        public static ComponentsModule forComponents(Component<?>...components) {
            return forComponents(Arrays.asList(components));
        }
        
        public static ComponentsModule forComponents(Iterable<? extends Component<?>> components) {
            return new ComponentsModule(components);
        }
        
        protected final ImmutableList<Component<?>> components;
        
        public ComponentsModule(Iterable<? extends Component<?>> components) {
            this.components = ImmutableList.copyOf(components);
        }
        
        @Provides
        public List<Component<?>> getComponents() {
            return components;
        }
        
        @Override
        protected void configure() {
            bind(new TypeLiteral<Iterable<Component<?>>>(){}).to(new TypeLiteral<List<Component<?>>>(){});
        }
    }
    
    public static class AnnotatedComponentModule extends AbstractModule {

        public static AnnotatedComponentModule create(
                Component<?> component) {
            return new AnnotatedComponentModule(component);
        }
        
        protected final Component<?> component;
        
        protected AnnotatedComponentModule(
                Component<?> component) {
            this.component = component;
        }

        @Override
        protected void configure() {
            bind(new TypeLiteral<Component<?>>(){}).annotatedWith(component.annotation()).toInstance(component);
            bind(Component.class).annotatedWith(component.annotation()).toInstance(component);
        }
    }
    
    public static class AnnotatedComponentsModule extends ComponentsModule {

        public static AnnotatedComponentsModule forComponents(Component<?>...components) {
            return forComponents(Arrays.asList(components));
        }
        
        public static AnnotatedComponentsModule forComponents(Iterable<? extends Component<?>> components) {
            return new AnnotatedComponentsModule(components);
        }
        
        public AnnotatedComponentsModule(Iterable<? extends Component<?>> components) {
            super(components);
        }
        
        @Override
        protected void configure() {
            super.configure();
            for (Component<?> component: components) {
                install(AnnotatedComponentModule.create(component));
            }
        }
    }
    
    public static class AnnotationModule<T extends Annotation> extends AbstractModule {

        public static <T extends Annotation> AnnotationModule<T> create(T annotation) {
            return new AnnotationModule<T>(annotation);
        }
        
        protected final T annotation;
        
        protected AnnotationModule(T annotation) {
            this.annotation = annotation;
        }

        @Override
        protected void configure() {
            bind(Annotation.class).toInstance(annotation);
        }
    }
    
    public static class NamedModule extends AnnotationModule<Named> {
        
        public static NamedModule create(Named name) {
            return new NamedModule(name);
        }
        
        protected NamedModule(Named name) {
            super(name);
        }

        @Override
        protected void configure() {
            bind(Annotation.class).to(Named.class);
            bind(Named.class).toInstance(annotation);
        }
    }
    
    
    public static class RuntimeModuleProvider implements Provider<RuntimeModule> {

        public static Module module() {
            return Module.create();
        }
        
        public static class Module extends AbstractModule {

            public static Module create() {
                return new Module();
            }
            
            protected Module() {}
            
            @Override
            protected void configure() {
                bind(RuntimeModule.class).toProvider(Modules.RuntimeModuleProvider.class).in(Singleton.class);
            }
        }
        
        protected final @Named("root") Component<?> parent;
        protected final Named name;
        
        @Inject
        public RuntimeModuleProvider(
                @Named("root") Component<?> parent,
                Named name) {
            this.parent = parent;
            this.name = name;
        }
        
        @Override
        public RuntimeModule get() {
            Binding<Configuration> configurationBinding = parent.injector().getExistingBinding(Key.get(Configuration.class));
            Configuration configuration = (configurationBinding != null) ? Configuration.fromConfiguration(configurationBinding.getProvider().get()) : Configuration.createEmpty();
            SingleThreadScheduledExectorFactory executor = DefaultRuntimeModule.SingleThreadScheduledExectorFactory.fromThreadFactoryBuilder(new ThreadFactoryBuilder().setNameFormat(name.value()));
            RuntimeModule runtime = DefaultRuntimeModule.create(
                    configuration, 
                    ServiceMonitor.defaults(), 
                    ListeningExecutorServiceFactory.newInstance(executor, executor),
                    DefaultRuntimeModule.ShutdownTimeoutConfiguration.get(configuration));
            return runtime;
        }
    }
    
    public static class ServiceComponentProvider implements Provider<Component<?>> {

        public static Module module(
                Class<? extends Provider<List<com.google.inject.Module>>> modules) {
            return module(ServiceComponentProvider.class, modules);
        }
        
        public static Module module(
                Class<? extends Provider<? extends Component<?>>> component,
                Class<? extends Provider<List<com.google.inject.Module>>> modules) {
            return Module.create(component, modules);
        }
        
        public static class Module extends AbstractModule {

            public static Module create(
                    Class<? extends Provider<? extends Component<?>>> component,
                    Class<? extends Provider<List<com.google.inject.Module>>> modules) {
                return new Module(component, modules);
            }

            protected final Class<? extends Provider<? extends Component<?>>> component;
            protected final Class<? extends Provider<List<com.google.inject.Module>>> modules;
            
            protected Module(
                    Class<? extends Provider<? extends Component<?>>> component,
                    Class<? extends Provider<List<com.google.inject.Module>>> modules) {
                this.component = component;
                this.modules = modules;
            }
            
            @Override
            protected void configure() {
                bind(new TypeLiteral<Component<?>>(){}).toProvider(component);
                bind(new TypeLiteral<List<com.google.inject.Module>>(){}).toProvider(modules);
            }
        }
        
        protected final @Named("root") Component<?> parent;
        protected final Annotation annotation;
        protected final Provider<List<com.google.inject.Module>> modules;
        
        @Inject
        public ServiceComponentProvider(
                @Named("root") Component<?> parent,
                Annotation annotation,
                Provider<List<com.google.inject.Module>> modules) {
            this.parent = parent;
            this.annotation = annotation;
            this.modules = modules;
        }
        
        @Override
        public Component<?> get() {
            final Component<?> component = parent.createChildComponent(
                    annotation, 
                    modules.get());
            component.injector().getInstance(Service.class);
            return component;
        }
    }
    
    public static class SafariServerModuleProvider implements Provider<List<com.google.inject.Module>> {

        public static Module module(
                List<SafariModule> modules) {
            return Module.create(modules);
        }
        
        public static class Module extends AbstractModule {

            public static Module create(
                    List<SafariModule> modules) {
                return new Module(modules);
            }

            protected final List<SafariModule> modules;
            
            protected Module(
                    List<SafariModule> modules) {
                this.modules = modules;
            }
            
            @Override
            protected void configure() {
                bind(new TypeLiteral<List<SafariModule>>(){}).toInstance(modules);
            }
        }
        
        protected final RuntimeModule runtime;
        protected final List<SafariModule> safariModules;
        
        @Inject
        public SafariServerModuleProvider(
                RuntimeModule runtime,
                List<SafariModule> safariModules) {
            this.runtime = runtime;
            this.safariModules = safariModules;
        }
        
        @Override
        public List<com.google.inject.Module> get() {
            return ImmutableList.<com.google.inject.Module>builder()
                    .add(GuiceRuntimeModule.create(runtime))
                    .add(AnonymousClientModule.create())
                    .add(SafariServicesModule.create(safariModules))
                    .add(new AbstractModule() {
                        @Override
                        protected void configure() {
                            bind(new TypeLiteral<List<Service>>(){}).to(Key.get(new TypeLiteral<List<Service>>(){}, Safari.class));
                        }
                    })
                    .add(ServiceModule.instantiates(Key.get(new TypeLiteral<List<Service>>(){}), ServiceMonitor.class)).build();
        }
    }
    
    public static abstract class DelegatingComponentProvider implements Provider<Component<?>> {
        
        protected final Provider<? extends Component<?>> delegate;

        protected DelegatingComponentProvider(
                Injector injector,
                Class<? extends Provider<? extends Component<?>>> delegate) {
            this(injector.getInstance(delegate));
        }
        
        protected DelegatingComponentProvider(
                Provider<? extends Component<?>> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public Component<?> get() {
            return delegate.get();
        }
    }
    
    public static class ServerModulesProvider implements Provider<List<com.google.inject.Module>> {

        protected final RuntimeModule runtime;
        
        @Inject
        public ServerModulesProvider(
                RuntimeModule runtime) {
            this.runtime = runtime;
        }

        @Override
        public List<com.google.inject.Module> get() {
            return ImmutableList.<com.google.inject.Module>of(
                    GuiceRuntimeModule.create(runtime), 
                    SimpleServerModule.create(),
                    ServiceModule.instantiates(Key.get(SimpleServerBuilder.class), ServiceMonitor.class));
        }
    }
    
    public static class ClientModulesProvider implements Provider<List<com.google.inject.Module>> {

        protected final Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider;
        protected final RuntimeModule runtime;
        
        @Inject
        public ClientModulesProvider(
                RuntimeModule runtime) {
            this(runtime, SimpleEnsembleClientProvider.class);
        }
        
        protected ClientModulesProvider(
                RuntimeModule runtime,
                Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider) {
            this.provider = provider;
            this.runtime = runtime;
        }

        @Override
        public List<com.google.inject.Module> get() {
            return ImmutableList.<com.google.inject.Module>of(
                    GuiceRuntimeModule.create(runtime), 
                    SimpleClientProvider.singletonModule(provider),
                    ServiceModule.instantiates(Key.get(ConnectionClientExecutorService.Builder.class), ServiceMonitor.class));
        }
    }
    
    public static class ComponentMonitorProvider extends AbstractModule {

        public static ComponentMonitorProvider create(
                Class<? extends Provider<ServiceMonitor>> monitor) {
            return new ComponentMonitorProvider(monitor);
        }
        
        protected final Class<? extends Provider<ServiceMonitor>> monitor;
        
        protected ComponentMonitorProvider(
                Class<? extends Provider<ServiceMonitor>> monitor) {
            this.monitor = monitor;
        }
        
        @Override
        protected void configure() {
            bind(new TypeLiteral<List<Service>>(){}).toProvider(Modules.ComponentServicesProvider.class);
            bind(ServiceMonitor.class).toProvider(monitor);
            install(ServiceModule.forType(ServiceMonitor.class));
        }
    }
    
    protected Modules() {}
}
