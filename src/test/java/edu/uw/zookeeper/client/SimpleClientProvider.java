package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.client.ClientConnectionFactoryBuilder;

public class SimpleClientProvider implements Provider<ConnectionClientExecutorService.Builder> {

    public static Module module() {
        return module(SimpleClientProvider.class);
    }

    public static Module module(Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider) {
        return Module.create(provider);
    }

    public static AsSingletonModule singletonModule() {
        return singletonModule(SimpleClientProvider.class);
    }
    
    public static AsSingletonModule singletonModule(Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider) {
        return AsSingletonModule.create(provider);
    }
    
    public static class Module extends AbstractModule {

        public static Module create(Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider) {
            return new Module(provider);
        }
        
        protected final Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider;
        
        protected Module(Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider) {
            this.provider = provider;
        }
        
        @Override
        protected void configure() {
            bind(ConnectionClientExecutorService.Builder.class).toProvider(provider);
        }
    }
    
    public static class AsSingletonModule extends Module {
        
        public static AsSingletonModule create(Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider) {
            return new AsSingletonModule(provider);
        }
        
        protected AsSingletonModule(Class<? extends Provider<ConnectionClientExecutorService.Builder>> provider) {
            super(provider);
        }
        
        @Override
        protected void configure() {
            bind(ConnectionClientExecutorService.Builder.class).toProvider(provider).in(Singleton.class);
        }
    }
    
    protected final NetClientModule clientModule;
    protected final RuntimeModule runtime;
    
    @Inject
    public SimpleClientProvider(
            NetClientModule clientModule,
            RuntimeModule runtime) {
        this.clientModule = clientModule;
        this.runtime = runtime;
    }
    
    @Override
    public ConnectionClientExecutorService.Builder get() {
        ConnectionClientExecutorService.Builder instance = ConnectionClientExecutorService.builder().setConnectionBuilder(ClientConnectionFactoryBuilder.defaults().setClientModule(clientModule)).setRuntimeModule(runtime).setDefaults();
        for (Service e: instance.build()) {
            runtime.getServiceMonitor().add(e);
        }
        return instance;
    }
}
