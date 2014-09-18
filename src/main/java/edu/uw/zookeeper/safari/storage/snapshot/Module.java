package edu.uw.zookeeper.safari.storage.snapshot;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;

import edu.uw.zookeeper.safari.AbstractCompositeSafariModule;

public class Module extends AbstractCompositeSafariModule<Service> {

    public static Class<Snapshot> annotation() {
        return Snapshot.class;
    }

    public static Module create() {
        SnapshotListener.Module module = SnapshotListener.module();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                        module,
                        FrontendSessionLookup.module(),
                        RoleListener.module()));
    }
    
    protected Module(
            Key<? extends Service> key,
            Iterable<? extends com.google.inject.Module> modules) {
        super(key, modules);
    }

    @Override  
    public Key<? extends Service> getKey() {
        return Key.get(Service.class, annotation());
    }
    
    @Provides @Snapshot
    public Service getService(
            Injector injector) {
        return getInstance(injector);
    }
}
