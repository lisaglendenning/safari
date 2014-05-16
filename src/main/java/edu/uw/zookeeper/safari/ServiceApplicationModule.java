package edu.uw.zookeeper.safari;


import java.util.List;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class ServiceApplicationModule extends AbstractModule {

    public static ServiceApplicationModule create() {
        return new ServiceApplicationModule();
    }
    
    protected ServiceApplicationModule() {}

    @Provides @Singleton
    public ServiceApplication getServiceApplication(
            List<Service> services,
            ServiceMonitor monitor) {
        for (Service service: services) {
            monitor.add(service);
        }
        return ServiceApplication.forService(monitor);
    }

    @Override
    protected void configure() {
        bind(Application.class).to(ServiceApplication.class);
    }
}
