package edu.uw.zookeeper.safari.frontend;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;

import edu.uw.zookeeper.safari.AbstractCompositeSafariModule;

public class Module extends AbstractCompositeSafariModule<Service> {

    public static Class<Frontend> annotation() {
        return Frontend.class;
    }

    public static Module create() {
        FrontendServerService.Module module = FrontendServerService.module();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                        RegionsConnectionsService.module(),
                        XomegaCache.module(),
                        FrontendServerExecutor.create(),
                        FrontendConnections.create(),
                        module));
    }
    
    protected Module(
            Key<? extends Service> key,
            Iterable<? extends com.google.inject.Module> modules) {
        super(key, modules);
    }

    @Override  
    public Key<Service> getKey() {
        return Key.get(Service.class, annotation());
    }
    
    @Provides @Frontend
    public Service getService(Injector injector) {
        return getInstance(injector);
    }
}
