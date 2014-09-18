package edu.uw.zookeeper.safari.region;

import java.util.List;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;

import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.safari.SafariModule;

public class InjectingRoleListener<T> extends AbstractRoleListener<T> {

    public static <T> InjectingRoleListener<T> listen(
            Injector injector,
            Function<RegionRoleService, ? extends List<? extends com.google.inject.Module>> modules,
            Supplier<FutureTransition<RegionRoleService>> role,
            T delegate,
            Logger logger,
            Service service) {
        return Services.listen(new InjectingRoleListener<T>(injector, modules, role, delegate, logger), service);
    }
    
    protected final Injector injector;
    protected final Function<RegionRoleService, ? extends List<? extends com.google.inject.Module>> modules;

    protected InjectingRoleListener(
            Injector injector,
            Function<RegionRoleService, ? extends List<? extends com.google.inject.Module>> modules,
            Supplier<FutureTransition<RegionRoleService>> role,
            T delegate,
            Logger logger) {
        super(role, delegate, logger);
        this.injector = injector;
        this.modules = modules;
    }
    
    protected InjectingRoleListener(
            Injector injector,
            Function<RegionRoleService, ? extends List<? extends com.google.inject.Module>> modules,
            Supplier<FutureTransition<RegionRoleService>> role,
            T delegate) {
        super(role, delegate);
        this.injector = injector;
        this.modules = modules;
    }
    
    protected InjectingRoleListener(
            Injector injector,
            Function<RegionRoleService, ? extends List<? extends com.google.inject.Module>> modules,
            Supplier<FutureTransition<RegionRoleService>> role,
            Logger logger) {
        super(role, logger);
        this.injector = injector;
        this.modules = modules;
    }
    
    protected InjectingRoleListener(
            Injector injector,
            Function<RegionRoleService, ? extends List<? extends com.google.inject.Module>> modules,
            Supplier<FutureTransition<RegionRoleService>> role) {
        super(role);
        this.injector = injector;
        this.modules = modules;
    }

    @Override
    public void onSuccess(final RegionRoleService result) {
        super.onSuccess(result);
        List<? extends com.google.inject.Module> modules = this.modules.apply(result);
        if (!modules.isEmpty()) {
            final Injector child = injector.createChildInjector(modules);
            for (com.google.inject.Module module: modules) {
                if (module instanceof SafariModule) {
                    child.getInstance(((SafariModule) module).getKey());
                }
            }
        }
    }
    
    public static class RegionRoleServiceModule extends AbstractModule {

        public static RegionRoleServiceModule create(
                RegionRoleService service) {
            return new RegionRoleServiceModule(service);
        }
        
        protected final RegionRoleService service;
        
        protected RegionRoleServiceModule(
                RegionRoleService service) {
            this.service = service;
        }

        @Override
        protected void configure() {
            bind(RegionRoleService.class).toInstance(service);
        }
    }
}
