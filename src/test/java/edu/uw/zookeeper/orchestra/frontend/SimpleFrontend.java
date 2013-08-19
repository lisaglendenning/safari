package edu.uw.zookeeper.orchestra.frontend;

public class SimpleFrontend extends FrontendServerService.Module {

    public static SimpleFrontend create() {
        return new SimpleFrontend();
    }
    
    @Override
    protected com.google.inject.Module[] getModules() {
        com.google.inject.Module[] modules = { 
                EnsembleConnectionsService.module(),
                SimpleFrontendConfiguration.create(),
                FrontendServerExecutor.module() };
        return modules;
    }
}
