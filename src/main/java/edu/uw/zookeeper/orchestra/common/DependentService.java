package edu.uw.zookeeper.orchestra.common;

import java.util.Iterator;
import java.util.concurrent.Executor;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;

import edu.uw.zookeeper.common.ServiceMonitor;

public abstract class DependentService extends AbstractIdleService {

    public static Iterator<Class<?>> dependentServiceTypes(DependsOn depends) {
        return Iterators.filter(
                Iterators.forArray(depends.value()), 
                new Predicate<Class<?>>() {
                    @Override
                    public boolean apply(Class<?> input) {
                        return Service.class.isAssignableFrom(input);
                    }
                });
    }
    
    public static void start(Injector injector, DependsOn depends) {
        Iterator<Class<?>> types = dependentServiceTypes(depends);
        while (types.hasNext()) {
            @SuppressWarnings("unchecked")
            Class<? extends Service> next = (Class<? extends Service>) types.next();
            start(injector, next);
        }
    }

    public static void start(Injector injector, Class<? extends Service> type) {
        DependsOn depends = type.getAnnotation(DependsOn.class);
        if (depends != null) {
            start(injector, depends);
        }
        Service service = getInstance(injector, type);
        switch (service.state()) {
        case NEW:
            service.startAsync();
        case STARTING:
            service.awaitRunning();
        case RUNNING:
            break;
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(String.valueOf(service));
        case FAILED:
            throw new IllegalStateException(service.failureCause());
        }
    }
    
    public static <T extends Service> T getInstance(Injector injector, Class<T> type) {
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        T service = injector.getInstance(type);
        if (! monitor.isMonitoring(service)) {
            monitor.add(service);
        }
        return service;
    }
    
    public static <T extends Service> T addOnStart(Injector injector, T service) {
        start(injector, service.getClass().getAnnotation(DependsOn.class));
        injector.getInstance(ServiceMonitor.class).addOnStart(service);
        return service;
    }
    
    protected final Injector injector;
    
    protected DependentService(Injector injector) {
        this.injector = injector;
    }
    
    protected Injector injector() {
        return injector;
    }

    @Override
    protected Executor executor() {
        return MoreExecutors.sameThreadExecutor();
    }
    
    @Override
    protected void startUp() throws Exception {
        addOnStart(injector, this);
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
