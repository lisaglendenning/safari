package edu.uw.zookeeper.orchestra.common;

import java.util.Iterator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;

@Singleton
public class DependentServiceMonitor {

    public static DependentServiceMonitor create(
            ServiceMonitor monitor) {
        return new DependentServiceMonitor(monitor);
    }

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
    
    private final ServiceMonitor monitor;

    @Inject
    public DependentServiceMonitor(
            ServiceMonitor monitor) {
        this.monitor = monitor;
    }

    public void start(Injector injector, DependsOn depends) {
        Iterator<Class<?>> types = dependentServiceTypes(depends);
        while (types.hasNext()) {
            @SuppressWarnings("unchecked")
            Class<? extends Service> next = (Class<? extends Service>) types.next();
            start(injector, next);
        }
    }
    
    public void start(Injector injector, Class<? extends Service> type) {
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
    
    public <T extends Service> T getInstance(Injector injector, Class<T> type) {
        T service = injector.getInstance(type);
        if (! monitor.isMonitoring(service)) {
            monitor.add(service);
        }
        return service;
    }
}
