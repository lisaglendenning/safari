package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.orchestra.netty.NettyModule;
import edu.uw.zookeeper.orchestra.protocol.ConductorCodecConnection;
import edu.uw.zookeeper.orchestra.protocol.JacksonModule;
import edu.uw.zookeeper.orchestra.protocol.MessagePacket;
import edu.uw.zookeeper.protocol.Operation;

public class ConductorService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public ConductorConfiguration getConductorConfiguration(
                ControlClientService controlClient, RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            return ConductorConfiguration.fromRuntime(controlClient, runtime);
        }

        @Provides @Singleton
        public EnsembleConfiguration getEnsembleConfiguration(
                BackendConfiguration backendConfiguration, 
                ControlClientService controlClient) throws InterruptedException, ExecutionException, KeeperException {
            return EnsembleConfiguration.fromRuntime(backendConfiguration, controlClient);
        }
        
        @Provides @Singleton
        public EnsembleMember getEnsembleMember(
                EnsembleConfiguration ensembleConfiguration,
                ConductorConfiguration conductorConfiguration,
                ServiceLocator locator,
                RuntimeModule runtime) {
            Orchestra.Ensembles.Entity myEnsemble = Orchestra.Ensembles.Entity.of(ensembleConfiguration.get());
            Orchestra.Ensembles.Entity.Conductors.Member myMember = Orchestra.Ensembles.Entity.Conductors.Member.of(
                    conductorConfiguration.get().id(), 
                    Orchestra.Ensembles.Entity.Conductors.of(myEnsemble));
            EnsembleMember instance = new EnsembleMember(myMember, myEnsemble, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }

        @Provides @Singleton
        public ConductorPeerService getConductorPeerService(
                ConductorConfiguration configuration,
                NettyModule netModule,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            ServerConnectionFactory<MessagePacket, ConductorCodecConnection> serverConnections = 
                    netModule.servers().get(
                            ConductorCodecConnection.codecFactory(JacksonModule.getMapper()), 
                            ConductorCodecConnection.factory())
                    .get(configuration.get().address().get());
            runtime.serviceMonitor().addOnStart(serverConnections);
            ClientConnectionFactory<MessagePacket, ConductorCodecConnection> clientConnections =  
                    netModule.clients().get(
                        ConductorCodecConnection.codecFactory(JacksonModule.getMapper()), 
                        ConductorCodecConnection.factory()).get();
            runtime.serviceMonitor().addOnStart(clientConnections);
            return new ConductorPeerService(serverConnections, clientConnections);
        }

        @Provides @Singleton
        public ConductorService getConductorService(
                ConductorConfiguration configuration,
                ConductorPeerService connections,
                EnsembleMember member,
                ServiceLocator locator) throws InterruptedException, ExecutionException, KeeperException {
            ConductorService instance = new ConductorService(configuration.get(), connections, member, locator);
            return instance;
        }
    }
    
    protected final ServiceLocator locator;
    protected final ConductorAddressView view;
    protected final ConductorPeerService connections;
    protected final EnsembleMember member;
    
    protected ConductorService(
            ConductorAddressView view,
            ConductorPeerService connections,
            EnsembleMember member,
            ServiceLocator locator) {
        this.view = view;
        this.connections = connections;
        this.member = member;
        this.locator = locator;
    }
    
    public ConductorAddressView view() {
        return view;
    }

    protected void register() throws KeeperException, InterruptedException, ExecutionException {
        Materializer materializer = locator.getInstance(ControlClientService.class).materializer();
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.of(view().id());
        Orchestra.Conductors.Entity.Presence presenceNode = Orchestra.Conductors.Entity.Presence.of(entityNode);
        Operation.SessionResult result = materializer.operator().create(presenceNode.path()).submit().get();
        Operations.unlessError(result.reply().reply(), result.toString());
    }
    
    @Override
    protected void startUp() throws Exception {
        locator.getInstance(ControlClientService.class).start().get();
        connections.start().get();
        register();
        member.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
