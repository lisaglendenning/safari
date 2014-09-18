package edu.uw.zookeeper.safari.volumes;

import com.google.common.collect.ImmutableList;
import com.google.inject.Key;

import edu.uw.zookeeper.safari.AbstractCompositeSafariModule;

public class Module extends AbstractCompositeSafariModule<Object> {

    public static Class<Volumes> annotation() {
        return Volumes.class;
    }

    public static Module create() {
        RoleListener.Module module = RoleListener.module();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                        module));
    }
    
    protected Module(
            Key<?> key,
            Iterable<? extends com.google.inject.Module> modules) {
        super(key, modules);
    }

    @Override  
    public Key<?> getKey() {
        return key;
    }
}
