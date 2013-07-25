package edu.uw.zookeeper.orchestra;

import com.google.inject.Inject;
import com.google.inject.Injector;

import edu.uw.zookeeper.util.Reference;

public class InjectorServiceLocator implements ServiceLocator, Reference<Injector> {

    protected final Injector injector;
    
    @Inject
    public InjectorServiceLocator(Injector injector) {
        this.injector = injector;
    }
    
    @Override
    public <T> T getInstance(Class<T> type) {
        return get().getInstance(type);
    }
    
    @Override
    public Injector get() {
        return injector;
    }
}
