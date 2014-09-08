package edu.uw.zookeeper.safari;

import java.lang.annotation.Annotation;

import com.google.common.base.MoreObjects;
import com.google.inject.Injector;
import com.google.inject.Module;

public class Component<T extends Annotation> {

    public static <T extends Annotation> Component<T> create(T annotation, Injector injector) {
        return new Component<T>(annotation, injector);
    }
    
    private final T annotation;
    private final Injector injector;
    
    public Component(T annotation, Injector injector) {
        this.annotation = annotation;
        this.injector = injector;
    }
    
    public T annotation() {
        return annotation;
    }
    
    public Injector injector() {
        return injector;
    }
    
    public <U extends Annotation> Component<U> createChildComponent(U annotation, Module...modules) {
        return create(annotation, injector().createChildInjector(modules));
    }
    
    public <U extends Annotation> Component<U> createChildComponent(U annotation, Iterable<? extends com.google.inject.Module> modules) {
        return create(annotation, injector().createChildInjector(modules));
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(annotation()).addValue(injector()).toString();
    }
}
