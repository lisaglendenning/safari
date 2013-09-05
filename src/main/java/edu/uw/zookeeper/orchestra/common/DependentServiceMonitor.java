package edu.uw.zookeeper.orchestra.common;

import java.util.Iterator;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import edu.uw.zookeeper.clients.common.ServiceLocator;
import edu.uw.zookeeper.common.ServiceMonitor;

@Singleton
public class DependentServiceMonitor {

    public static DependentServiceMonitor create(
            ServiceMonitor monitor,
            ServiceLocator locator) {
        return new DependentServiceMonitor(monitor, locator);
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
    private final ServiceLocator locator;

    @Inject
    public DependentServiceMonitor(
            ServiceMonitor monitor,
            ServiceLocator locator) {
        this.monitor = monitor;
        this.locator = locator;
    }

    public void start(DependsOn depends) {
        Iterator<Class<?>> types = dependentServiceTypes(depends);
        while (types.hasNext()) {
            @SuppressWarnings("unchecked")
            Class<? extends Service> next = (Class<? extends Service>) types.next();
            start(next);
        }
    }
    
    public void start(Class<? extends Service> type) {
        DependsOn depends = type.getAnnotation(DependsOn.class);
        if (depends != null) {
            start(depends);
        }
        Service service = getInstance(type);
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
    
    public <T extends Service> T getInstance(Class<T> type) {
        T service = locator.getInstance(type);
        if (! monitor.isMonitoring(service)) {
            monitor.add(service);
        }
        return service;
    }
}
