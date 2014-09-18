package edu.uw.zookeeper.safari;


import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.common.GuiceRuntimeModule;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.AnonymousClientModule;
import edu.uw.zookeeper.net.IntraVmModule;
import edu.uw.zookeeper.net.NettyModule;

public class Module extends ServiceApplicationModule {

    public static Class<Safari> annotation() {
        return Safari.class;
    }
    
    public static Injector createInjector(RuntimeModule runtime) {
        return Guice.createInjector(
                GuiceRuntimeModule.create(runtime),
                IntraVmModule.create(),
                NettyModule.create(),
                AnonymousClientModule.create(),
                SafariServicesModule.create(newSafariModules()),
                create());
    }

    public static Module create() {
        return new Module();
    }
    
    public static ImmutableList<? extends SafariModule> newSafariModules() {
        return ImmutableList.of(
                edu.uw.zookeeper.safari.control.Module.create(),
                edu.uw.zookeeper.safari.storage.Module.create(),
                edu.uw.zookeeper.safari.peer.Module.create(),
                edu.uw.zookeeper.safari.region.Module.create(),
                edu.uw.zookeeper.safari.control.volumes.Module.create(),
                edu.uw.zookeeper.safari.storage.volumes.Module.create(),
                edu.uw.zookeeper.safari.storage.snapshot.Module.create(),
                edu.uw.zookeeper.safari.volumes.Module.create(),
                edu.uw.zookeeper.safari.backend.Module.create(),
                edu.uw.zookeeper.safari.frontend.Module.create());
    }

    protected Module() {}

    @Override
    protected void configure() {
        bind(new TypeLiteral<List<Service>>(){}).to(Key.get(new TypeLiteral<List<Service>>(){}, annotation()));
        super.configure();
    }
}
