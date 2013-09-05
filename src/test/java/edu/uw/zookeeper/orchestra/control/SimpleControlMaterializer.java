package edu.uw.zookeeper.orchestra.control;

import java.util.List;

import com.google.inject.internal.ImmutableList;

public class SimpleControlMaterializer extends ControlMaterializerService.Module {

    public static SimpleControlMaterializer create() {
        return new SimpleControlMaterializer();
    }
    
    public SimpleControlMaterializer() {
    }

    @Override
    protected List<com.google.inject.Module> getDependentModules() {
        return ImmutableList.<com.google.inject.Module>of(SimpleControlConnections.module());
    }
}