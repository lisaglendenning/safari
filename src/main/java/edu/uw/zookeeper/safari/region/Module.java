package edu.uw.zookeeper.safari.region;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;

import edu.uw.zookeeper.safari.AbstractCompositeSafariModule;

public class Module extends AbstractCompositeSafariModule<Service> {

    public static Class<Region> annotation() {
        return Region.class;
    }

    public static Module create() {
        RegionMemberService.Module module = RegionMemberService.module();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                    RegionConfiguration.create(),
                    RegionRoleService.module(),
                    module));
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
    
    @Provides @Region
    public Service getService(
            Injector injector) {
        return getInstance(injector);
    }
}
