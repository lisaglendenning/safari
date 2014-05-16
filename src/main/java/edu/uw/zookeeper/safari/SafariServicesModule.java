package edu.uw.zookeeper.safari;


import java.lang.annotation.Annotation;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.common.ServiceMonitor;

public class SafariServicesModule extends AbstractSafariModule {

    public static Class<Safari> annotation() {
        return Safari.class;
    }

    public static SafariServicesModule create(
            Iterable<? extends SafariModule> modules) {
        return new SafariServicesModule(modules);
    }
    
    private final ImmutableList<SafariModule> modules;
    
    protected SafariServicesModule(
            Iterable<? extends SafariModule> modules) {
        this.modules = ImmutableList.copyOf(modules);
    }
    
    @Provides @Singleton @Safari
    public List<Class<? extends Annotation>> getAnnotations(
           @Safari List<SafariModule> modules) {
        ImmutableList.Builder<Class<? extends Annotation>> annotations = ImmutableList.builder();
        for (SafariModule module: modules) {
            annotations.add(module.getAnnotation());
        }
        return annotations.build();
    }
    
    @Provides @Singleton @Safari
    public List<Service> getSafariServices(
            @Safari List<Class<? extends Annotation>> annotations,
            Injector injector) {
        ImmutableList.Builder<Service> services = ImmutableList.builder();
        for (Class<? extends Annotation> annotation: annotations) {
            services.add(injector.getInstance(Key.get(Service.class, annotation)));
        }
        return services.build();
    }
    
    @Provides @Singleton @Safari
    public List<SafariModule> getSafariModules() {
        return getModules();
    }

    @Override
    public Class<? extends Annotation> getAnnotation() {
        return annotation();
    }
    
    @Override
    protected List<SafariModule> getModules() {
        return modules;
    }

    @Override
    protected Class<? extends Service> getServiceType() {
        return ServiceMonitor.class;
    }
}
