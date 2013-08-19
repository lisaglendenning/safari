package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Injector;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.orchestra.common.ServiceLocator;

@RunWith(JUnit4.class)
public class BootstrapTest {

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = SimpleMain.injector();
        SimpleMain.start(injector.getInstance(ServiceLocator.class));
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        Thread.sleep(500);
        monitor.stop().get();
    }
}
