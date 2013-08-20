package edu.uw.zookeeper.orchestra.net;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.FixedClientConnectionFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.intravm.IntraVmNetModule;
import edu.uw.zookeeper.orchestra.ClientConnectionsModule;
import edu.uw.zookeeper.orchestra.RuntimeModuleProvider;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.server.SimpleServer;

public class SimpleClient extends ClientConnectionsModule {
    
    public static Injector injector() {
        return Guice.createInjector(create());
    }
    
    public static SimpleClient create() {
        return new SimpleClient();
    }

    @Override
    protected void configure() {
        super.configure();
        bind(ClientConnectionFactory.class).to(new TypeLiteral<ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>(){});
        bind(FixedClientConnectionFactory.class).to(new TypeLiteral<FixedClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>>>(){});
    }

    @Override
    @Provides @Singleton
    public ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getClientConnectionFactory(
            NetClientModule clients) {
        return super.getClientConnectionFactory(clients);
    }
    
    @Provides @Singleton
    public FixedClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getFixedClientConnectionFactory(
            SimpleServer server,
            ClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> connections) {
        return FixedClientConnectionFactory.create(
                server.getConnections().connections().listenAddress(), 
                connections);
    }
    
    @Provides
    public ListenableFuture<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> getConnection(
            FixedClientConnectionFactory<? extends ProtocolCodecConnection<Operation.Request,AssignXidCodec,Connection<Operation.Request>>> factory) {
        return factory.get();
    }

    @Provides @Singleton
    public SimpleServer getServer(
            IntraVmNetModule module) {
        return SimpleServer.newInstance(module);
    }
    
    @Override
    protected Module[] getModules() {
        Module[] modules = { 
                RuntimeModuleProvider.create(),
                IntraVmAsNetModule.create() };
        return modules;
    }
    
    @DependsOn({ SimpleServer.class, ClientConnectionFactory.class })
    public static class SimpleClientService extends DependentService {

        @Inject
        public SimpleClientService(ServiceLocator locator) {
            super(locator);
        }
    }
}
