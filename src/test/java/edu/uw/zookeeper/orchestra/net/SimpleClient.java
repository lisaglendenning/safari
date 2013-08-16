package edu.uw.zookeeper.orchestra.net;

import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.orchestra.ClientConnectionsModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.server.SimpleServer;

public class SimpleClient extends ClientConnectionsModule {
    
    public static SimpleClient create() {
        return new SimpleClient();
    }

    @Override
    protected void configure() {
        super.configure();
        TypeLiteral<FixedClientConnectionFactory<? extends Connection<? super Message.ClientSession>>> generic = new TypeLiteral<FixedClientConnectionFactory<? extends Connection<? super Message.ClientSession>>>(){};
        bind(FixedClientConnectionFactory.class).to(generic);
    }

    @Provides @Singleton
    public FixedClientConnectionFactory<? extends Connection<? super Message.ClientSession>> getControlConnectionsService(
            SimpleServer server,
            NetClientModule clients) {
        return FixedClientConnectionFactory.create(
                server.getConnections().connections().listenAddress(), 
                getClientConnectionFactory(clients));
    }

    @Provides @Singleton
    public SimpleServer getServer(
            IntraVmNetModule module) {
        return SimpleServer.newInstance(module);
    }
    
    @Override
    protected Module[] getModules() {
        Module[] modules = { IntraVmDefaultsModule.create() };
        return modules;
    }
}
