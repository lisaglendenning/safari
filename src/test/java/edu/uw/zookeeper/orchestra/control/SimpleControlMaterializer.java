package edu.uw.zookeeper.orchestra.control;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

public class SimpleControlMaterializer extends AbstractModule {

    public static SimpleControlMaterializer create() {
        return new SimpleControlMaterializer();
    }
    
    public SimpleControlMaterializer() {
    }
    
    @Override
    protected void configure() {
        install(SimpleControlConfiguration.module());
        install(SimpleControlConnections.create());
        TypeLiteral<ControlMaterializerService<?>> generic = new TypeLiteral<ControlMaterializerService<?>>(){};
        bind(ControlMaterializerService.class).to(generic);
    }

    @Provides @Singleton
    public ControlMaterializerService<?> getControlClientService(
            ControlConnectionsService<?> connections) {
        return ControlMaterializerService.newInstance(connections);
    }
}