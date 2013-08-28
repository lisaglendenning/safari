package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Injector;

import edu.uw.zookeeper.common.ServiceMonitor;

@RunWith(JUnit4.class)
public class BootstrapTest {

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        Injector injector = SimpleMain.injector();
        injector.getInstance(SimpleMain.SimpleMainService.class).startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        Thread.sleep(500);
        monitor.stopAsync().awaitTerminated();
    }
}
