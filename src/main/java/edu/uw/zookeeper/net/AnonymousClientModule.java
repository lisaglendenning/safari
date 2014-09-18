package edu.uw.zookeeper.net;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.AnonymousClientConnection;

public class AnonymousClientModule extends AbstractModule {

    public static AnonymousClientModule create() {
        return new AnonymousClientModule();
    }
    
    protected AnonymousClientModule() {}
    
    @Provides @Singleton
    public ClientConnectionFactory<? extends AnonymousClientConnection<?,?>> getClientConnectionFactory(
            final NetClientModule clientModule,
            final ServiceMonitor monitor) {
        final ClientConnectionFactory<? extends AnonymousClientConnection<?,?>> connections = AnonymousClientConnection.defaults(clientModule);
        monitor.add(connections);
        return connections;
    }
    
    @Override
    protected void configure() {
        bind(new TypeLiteral<ClientConnectionFactory<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>>>(){}).to(new TypeLiteral<ClientConnectionFactory<? extends AnonymousClientConnection<?,?>>>(){});
    }
}
