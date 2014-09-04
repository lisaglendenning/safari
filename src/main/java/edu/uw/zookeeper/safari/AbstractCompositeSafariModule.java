package edu.uw.zookeeper.safari;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

public abstract class AbstractCompositeSafariModule<V> extends AbstractModule implements SafariModule {

    protected final ImmutableList<com.google.inject.Module> modules;
    protected final Key<? extends V> key;
    
    protected AbstractCompositeSafariModule(
            Key<? extends V> key,
            Iterable<? extends com.google.inject.Module> modules) {
        this.key = key;
        this.modules = ImmutableList.copyOf(modules);
    }
    
    @Override
    protected void configure() {
        for (Module module: modules) {
            install(module);
        }
    }
    
    protected V getInstance(
            Injector injector) {
        V instance = injector.getInstance(key);
        for (com.google.inject.Module module: modules) {
            if (module instanceof SafariModule) {
                injector.getInstance(((SafariModule) module).getKey());
            }
        }
        return instance;
    }
}
