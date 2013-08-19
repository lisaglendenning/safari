package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import edu.uw.zookeeper.orchestra.backend.BackendTest;
import edu.uw.zookeeper.orchestra.common.DependentServiceMonitor;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;
import edu.uw.zookeeper.orchestra.control.ControlTest;
import edu.uw.zookeeper.orchestra.frontend.FrontendTest;
import edu.uw.zookeeper.orchestra.net.IntraVmAsNetModule;
import edu.uw.zookeeper.orchestra.peer.PeerTest;

public class SimpleMain extends MainApplicationModule {

    public static Injector injector() {
        return Guice.createInjector(
                create());
    }

    public static void start(ServiceLocator locator) throws InterruptedException, ExecutionException {
        DependentServiceMonitor monitor = locator.getInstance(DependentServiceMonitor.class);
        MainService main = monitor.listen(locator.getInstance(MainService.class));
        monitor.get().add(main);
        monitor.get().start().get();
    }
    
    public static SimpleMain create() {
        return new SimpleMain(RuntimeModuleProvider.create());
    }
    
    public SimpleMain(RuntimeModuleProvider runtime) {
        super(runtime);
    }

    @Override
    protected Module[] getModules() {
        Module[] modules = { 
                runtime, 
                IntraVmAsNetModule.create(),
                ControlTest.module(),
                PeerTest.module(),
                BackendTest.module(),
                FrontendTest.module() };
        return modules;
    }
}
