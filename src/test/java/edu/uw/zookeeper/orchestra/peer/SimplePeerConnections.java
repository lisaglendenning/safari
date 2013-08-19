package edu.uw.zookeeper.orchestra.peer;

public class SimplePeerConnections extends PeerConnectionsService.Module {

    public static SimplePeerConnections create() {
        return new SimplePeerConnections();
    }
    
    @Override
    protected com.google.inject.Module[] getModules() {
        com.google.inject.Module[] modules = { SimplePeerConfiguration.create() };
        return modules;
    }
}
