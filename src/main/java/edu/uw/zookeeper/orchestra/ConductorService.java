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
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Pair;

public class ConductorService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(ConductorConfiguration.module());
            install(EnsemblePeerService.module());
            install(EnsembleMemberService.module());
            install(VolumeLookupService.module());
        }

        @Provides @Singleton
        public ConductorService getConductorService(
                ConductorConfiguration configuration,
                EnsemblePeerService connections,
                EnsembleMemberService member,
                ServiceLocator locator,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            ConductorService instance = new ConductorService(configuration.getAddress(), connections, member, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    protected final ServiceLocator locator;
    protected final ConductorAddressView view;
    protected final EnsemblePeerService connections;
    protected final EnsembleMemberService member;
    
    protected ConductorService(
            ConductorAddressView view,
            EnsemblePeerService connections,
            EnsembleMemberService member,
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
        Materializer<?,?> materializer = locator.getInstance(ControlClientService.class).materializer();
        Orchestra.Conductors.Entity entityNode = Orchestra.Conductors.Entity.of(view().id());
        Orchestra.Conductors.Entity.Presence presenceNode = Orchestra.Conductors.Entity.Presence.of(entityNode);
        Pair<? extends Operation.SessionRequest, ? extends Operation.SessionResponse> result = materializer.operator().create(presenceNode.path()).submit().get();
        Operations.unlessError(result.second().response(), result.toString());
    }
    
    @Override
    protected void startUp() throws Exception {
        connections.start().get();
        register();
        member.start().get();
        locator.getInstance(VolumeLookupService.class).start().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
