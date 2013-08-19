package edu.uw.zookeeper.orchestra.backend;

public class SimpleBackend extends BackendRequestService.Module {

    public static SimpleBackend create() {
        return new SimpleBackend();
    }
    
    public SimpleBackend() {
    }

    @Override
    protected com.google.inject.Module[] getModules() {
        com.google.inject.Module[] modules = { SimpleBackendConnections.create()};
        return modules;
    }
}