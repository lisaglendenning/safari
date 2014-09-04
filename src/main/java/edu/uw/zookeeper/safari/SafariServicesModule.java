package edu.uw.zookeeper.safari;


import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;

public class SafariServicesModule extends AbstractCompositeSafariModule<Service> {

    public static Class<Safari> annotation() {
        return Safari.class;
    }

    public static SafariServicesModule create(
            Iterable<? extends SafariModule> modules) {
        return new SafariServicesModule(modules);
    }
    
    protected SafariServicesModule(
            Iterable<? extends SafariModule> modules) {
        super(Key.get(ServiceMonitor.class, annotation()), modules);
    }
    
    @Provides @Singleton @Safari
    public List<Service> getSafariServices(
            Injector injector) {
        ImmutableList.Builder<Service> services = ImmutableList.builder();
        for (com.google.inject.Module module: modules) {
            Object instance = injector.getInstance(((SafariModule) module).getKey());
            if (instance instanceof Service) {
                services.add((Service) instance);
            }
        }
        return services.build();
    }
    
    @Override
    public Key<Service> getKey() {
        return Key.get(Service.class, annotation());
    }
    
    @Provides @Safari
    public Service getService(Injector injector) {
        return getInstance(injector);
    }
}
