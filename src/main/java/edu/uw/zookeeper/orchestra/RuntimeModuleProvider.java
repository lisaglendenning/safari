package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.AbstractMain.ListeningExecutorServiceFactory;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceMonitor;

public class RuntimeModuleProvider extends AbstractModule {

    public static RuntimeModuleProvider create(RuntimeModule runtime) {
        return new RuntimeModuleProvider(runtime);
    }
    
    protected final RuntimeModule runtime;
    
    public RuntimeModuleProvider(RuntimeModule runtime) {
        this.runtime = runtime;
    }
    
    @Override
    protected void configure() {
        bind(ServiceLocator.class).to(InjectorServiceLocator.class).in(Singleton.class);
        bind(ExecutorService.class).to(ListeningExecutorService.class).in(Singleton.class);
        bind(ScheduledExecutorService.class).to(ListeningScheduledExecutorService.class).in(Singleton.class);
    }

    @Provides @Singleton
    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Provides @Singleton
    public ServiceMonitor getServiceMonitor() {
        return runtime.serviceMonitor();
    }

    @Provides @Singleton
    public Configuration getConfiguration() {
        return runtime.configuration();
    }

    @Provides @Singleton
    public Factory<ThreadFactory> getThreadFactory() {
        return runtime.threadFactory();
    }

    @Provides @Singleton
    public Factory<Publisher> getPublisherFactory() {
        return runtime.publisherFactory();
    }

    @Provides @Singleton
    public ListeningExecutorServiceFactory getExecutors() {
        return runtime.executors();
    }
    
    @Provides @Singleton
    public ListeningExecutorService getListeningExecutor() {
        return runtime.executors().asListeningExecutorServiceFactory().get();
    }
    
    @Provides @Singleton
    public ListeningScheduledExecutorService getListeningScheduledExecutor() {
        return runtime.executors().asListeningScheduledExecutorServiceFactory().get();
    }
}
