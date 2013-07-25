package edu.uw.zookeeper.orchestra.control;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.server.SimpleServerConnections;

public class SimpleControlMaterializer extends AbstractModule {

    public static SimpleControlMaterializer create(SimpleServerConnections server) {
        return new SimpleControlMaterializer(server);
    }
    
    protected final SimpleServerConnections server;
    
    public SimpleControlMaterializer(SimpleServerConnections server) {
        this.server = server;
    }
    
    @Override
    protected void configure() {
        install(getControlConnectionsModule());
        TypeLiteral<ControlMaterializerService<?>> generic = new TypeLiteral<ControlMaterializerService<?>>(){};
        bind(ControlMaterializerService.class).to(generic);
    }

    @Provides @Singleton
    public ControlMaterializerService<?> getControlClientService(
            ControlConnectionsService<?> connections) {
        return ControlMaterializerService.newInstance(connections);
    }
    
    protected com.google.inject.Module getControlConnectionsModule() {
        return SimpleControlConnections.create(server);
    }
}