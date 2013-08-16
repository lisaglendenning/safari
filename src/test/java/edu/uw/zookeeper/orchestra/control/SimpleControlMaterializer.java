package edu.uw.zookeeper.orchestra.control;

public class SimpleControlMaterializer extends ControlMaterializerService.Module {

    public static SimpleControlMaterializer create() {
        return new SimpleControlMaterializer();
    }
    
    public SimpleControlMaterializer() {
    }

    @Override
    protected com.google.inject.Module[] getModules() {
        com.google.inject.Module[] modules = { SimpleControlConnections.create() };
        return modules;
    }
}