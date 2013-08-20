package edu.uw.zookeeper.orchestra;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;

import edu.uw.zookeeper.orchestra.backend.BackendTest;
import edu.uw.zookeeper.orchestra.common.DependentService;
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlTest;
import edu.uw.zookeeper.orchestra.frontend.FrontendTest;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;
import edu.uw.zookeeper.orchestra.peer.EnsembleMemberService;
import edu.uw.zookeeper.orchestra.peer.PeerTest;

public class SimpleMain extends DependentModule {

    public static Injector injector() {
        return Guice.createInjector(
                create());
    }

    public static SimpleMain create() {
        return new SimpleMain();
    }
    
    public SimpleMain() {
    }

    @Override
    protected Module[] getModules() {
        Module[] modules = { 
                RuntimeModuleProvider.create(), 
                IntraVmAsNetModule.create(),
                ControlTest.module(),
                PeerTest.module(),
                BackendTest.module(),
                FrontendTest.module() };
        return modules;
    }

    @Singleton
    @DependsOn({
        ControlTest.ControlTestService.class, 
        BackendTest.BackendTestService.class, 
        EnsembleMemberService.class,
        FrontendTest.FrontendTestService.class})
    public static class SimpleMainService extends DependentService {

        @Inject
        public SimpleMainService(ServiceLocator locator) {
            super(locator);
        }
    }
}
