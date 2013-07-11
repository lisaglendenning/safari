package edu.uw.zookeeper.orchestra;

import java.lang.reflect.AnnotatedElement;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Service;

public abstract class Dependencies {

    public static DependsOn dependsOn(Object obj) {
        AnnotatedElement annotated = (obj instanceof AnnotatedElement) 
                ? (AnnotatedElement) obj : obj.getClass();
        return annotated.getAnnotation(DependsOn.class);
    }
    
    public static Iterator<Service> dependencies(
            final Object obj, 
            final ServiceLocator locator) {
        Iterator<Service> services;
        DependsOn annotation = dependsOn(obj);
        if (annotation == null) {
            services = Iterators.emptyIterator();
        } else {
            services = Iterators.transform(
                    Iterators.forArray(annotation.value()),
                    new Function<Class<? extends Service>, Service>() {
                        @Override
                        public Service apply(Class<? extends Service> input) {
                            return locator.getInstance(input);
                        }});
        }
        return services;
    }
    
    public static void startDependenciesAndWait(Object obj, ServiceLocator locator) throws InterruptedException, ExecutionException {
        Iterator<Service> itr = dependencies(obj, locator);
        while (itr.hasNext()) {
            itr.next().start().get();
        }
    }

    public static void startDependencies(Object obj, ServiceLocator locator) throws InterruptedException, ExecutionException {
        Iterator<Service> itr = dependencies(obj, locator);
        while (itr.hasNext()) {
            itr.next().start();
        }
    }
    
    public static void startAllAndWait(Service service, ServiceLocator locator) throws InterruptedException, ExecutionException {
        startDependenciesAndWait(service, locator);
        service.start().get();
    }

    public static void startAll(Service service, ServiceLocator locator) throws InterruptedException, ExecutionException {
        startDependencies(service, locator);
        service.start();
    }
    
    private Dependencies() {}
}
