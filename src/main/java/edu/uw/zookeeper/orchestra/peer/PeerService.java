package edu.uw.zookeeper.orchestra.peer;

import java.util.concurrent.ExecutionException;
import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.orchestra.ServiceLocator;

public class PeerService extends AbstractIdleService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
            install(PeerConfiguration.module());
            install(EnsemblePeerService.module());
            install(EnsembleMemberService.module());
        }

        @Provides @Singleton
        public PeerService getPeerService(
                PeerConfiguration configuration,
                EnsembleMemberService member,
                ServiceLocator locator,
                RuntimeModule runtime) throws InterruptedException, ExecutionException, KeeperException {
            PeerService instance = new PeerService(configuration.getView(), member, locator);
            runtime.serviceMonitor().addOnStart(instance);
            return instance;
        }
    }
    
    public static PeerService newInstance(
            PeerAddressView view,
            EnsembleMemberService member,
            ServiceLocator locator) {
        PeerService instance = new PeerService(view, member, locator);
        return instance;
    }

    protected final ServiceLocator locator;
    protected final PeerAddressView view;
    protected final EnsembleMemberService member;
    
    protected PeerService(
            PeerAddressView view,
            EnsembleMemberService member,
            ServiceLocator locator) {
        this.view = view;
        this.member = member;
        this.locator = locator;
    }
    
    public PeerAddressView view() {
        return view;
    }

    @Override
    protected void startUp() throws Exception {
        member.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
