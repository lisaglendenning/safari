package edu.uw.zookeeper.safari.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import edu.uw.zookeeper.safari.AbstractCompositeSafariModule;

public class Module extends AbstractCompositeSafariModule<Service> {

    public static Class<Storage> annotation() {
        return Storage.class;
    }
    
    public static Module create() {
        StorageClientService module = StorageClientService.create();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                    StorageCfgFile.module(),
                    StorageEnsembleConnections.create(),
                    StorageServerConnections.create(),
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
    
    @Provides @Storage
    public Service getService(Injector injector) {
        return getInstance(injector);
    }
}
