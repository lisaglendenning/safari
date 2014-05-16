package edu.uw.zookeeper.safari;

import java.util.List;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;

public abstract class AbstractSafariModule extends AbstractModule implements SafariModule {

    @Override
    protected void configure() {
        for (Module module: getModules()) {
            install(module);
        }
        bind(Key.get(Service.class, getAnnotation())).to(getServiceType());
    }
    
    protected abstract List<? extends com.google.inject.Module> getModules();
    
    protected abstract Class<? extends Service> getServiceType();
}
