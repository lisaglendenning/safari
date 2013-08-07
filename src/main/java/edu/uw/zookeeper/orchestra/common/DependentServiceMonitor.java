package edu.uw.zookeeper.orchestra.common;

import java.util.Iterator;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.ServiceMonitor;

@Singleton
public class DependentServiceMonitor implements Reference<ServiceMonitor> {

    public static DependentServiceMonitor create(
            ServiceMonitor monitor,
            ServiceLocator locator) {
        return new DependentServiceMonitor(monitor, locator);
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
    
    @Override
    public ServiceMonitor get() {
        return monitor;
    }
    
    public <T extends Service> T listen(T service) {
        Listener listener = new Listener(service);
        service.addListener(listener, MoreExecutors.sameThreadExecutor());
        return service;
    }
    
    public <T extends Service> T add(T service) {
        Iterator<Service> itr = Dependencies.dependencies(service, locator);
        while (itr.hasNext()) {
            monitor.addOnStart(itr.next());
        }
        monitor.addOnStart(service);
        return service;
    }

    protected class Listener implements Service.Listener, Reference<Service> {
    
        private final Service service;
    
        public Listener(Service service) {
            this.service = service;
        }
        
        @Override
        public Service get() {
            return service;
        }
        
        @Override
        public void starting() {
        }
    
        @Override
        public void running() {
            add(get());
        }
    
        @Override
        public void stopping(Service.State from) {
        }
    
        @Override
        public void terminated(Service.State from) {
        }
    
        @Override
        public void failed(Service.State from, Throwable failure) {
        }
    }
}
