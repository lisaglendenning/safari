package edu.uw.zookeeper.safari.volumes;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Key;

import edu.uw.zookeeper.safari.AbstractCompositeSafariModule;

public class Module extends AbstractCompositeSafariModule<Service.Listener> {

    public static Class<Volumes> annotation() {
        return Volumes.class;
    }

    public static Module create() {
        RoleListener.Module module = RoleListener.module();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                        VolumeOperationExecutor.module(),
                        module));
    }
    
    protected Module(
            Key<? extends Service.Listener> key,
            Iterable<? extends com.google.inject.Module> modules) {
        super(key, modules);
    }

    @Override  
    public Key<? extends Service.Listener> getKey() {
        return key;
    }
}
