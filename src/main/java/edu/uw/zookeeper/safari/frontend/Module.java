package edu.uw.zookeeper.safari.frontend;

import java.lang.annotation.Annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.safari.AbstractSafariModule;

public class Module extends AbstractSafariModule {

    public static Class<Frontend> annotation() {
        return Frontend.class;
    }

    public static Module create() {
        return new Module();
    }
    
    public Module() {}

    @Override
    public Class<? extends Annotation> getAnnotation() {
        return annotation();
    }

    @Override    
    protected ImmutableList<? extends com.google.inject.Module> getModules() {
        return ImmutableList.<com.google.inject.Module>of(
                RegionsConnectionsService.module(),
                FrontendServerExecutor.create(),
                FrontendConnections.create(),
                FrontendServerService.module());
    }

    @Override  
    protected Class<? extends Service> getServiceType() {
        return FrontendServerService.class;
    }
}