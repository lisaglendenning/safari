package edu.uw.zookeeper.orchestra;

import com.google.inject.AbstractModule;
import com.google.inject.Module;

public abstract class DependentModule extends AbstractModule {

    @Override
    protected void configure() {
        for (Module m: getModules()) {
            install(m);
        }
    }

    protected abstract Module[] getModules();
}
