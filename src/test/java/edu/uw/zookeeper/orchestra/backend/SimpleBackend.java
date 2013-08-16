package edu.uw.zookeeper.orchestra.backend;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.PingingClient;

public class SimpleBackend extends AbstractModule {

    public static SimpleBackend create() {
        return new SimpleBackend();
    }
    
    public SimpleBackend() {
    }
    
    @Override
    protected void configure() {
        install(SimpleBackendConfiguration.module());
        install(SimpleBackendConnections.create());
        TypeLiteral<BackendRequestService<?>> generic = new TypeLiteral<BackendRequestService<?>>() {};
        bind(BackendRequestService.class).to(generic);
    }

    @Provides @Singleton
    public BackendRequestService<?> getBackendRequestService(
            BackendConnectionsService<PingingClient<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections) {
        return null;
    }
}