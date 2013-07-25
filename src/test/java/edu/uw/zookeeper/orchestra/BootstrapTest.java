package edu.uw.zookeeper.orchestra;


import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import edu.uw.zookeeper.server.SimpleServerConnections;
import edu.uw.zookeeper.server.SimpleServerExecutor;

@RunWith(JUnit4.class)
public class BootstrapTest {

    @Test(timeout=5000)
    public void test() throws InterruptedException, ExecutionException {
        SimpleServerExecutor server = SimpleServerExecutor.newInstance();
        SimpleServerConnections serverConnections = SimpleServerConnections.newInstance(server.getTasks());
        serverConnections.start().get();
        serverConnections.stop().get();
    }
}
