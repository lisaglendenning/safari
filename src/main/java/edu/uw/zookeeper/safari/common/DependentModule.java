package edu.uw.zookeeper.safari.common;

import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Module;

public abstract class DependentModule extends AbstractModule {

    @Override
    protected void configure() {
        installDependentModules();
    }
    
    protected void installDependentModules() {
        for (Module m: getDependentModules()) {
            install(m);
        }
    }

    protected abstract List<? extends Module> getDependentModules();
}
