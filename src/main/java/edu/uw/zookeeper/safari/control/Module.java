package edu.uw.zookeeper.safari.control;

import java.lang.annotation.Annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.safari.AbstractSafariModule;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;

public class Module extends AbstractSafariModule {

    public static Class<Control> annotation() {
        return Control.class;
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
                JacksonModule.create(),
                ControlEnsembleConnections.create(),
                ControlClientService.module());
    }

    @Override  
    protected Class<? extends Service> getServiceType() {
        return ControlClientService.class;
    }
}
