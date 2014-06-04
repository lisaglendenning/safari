package edu.uw.zookeeper.safari.data;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.control.ControlModules;

@RunWith(JUnit4.class)
public class DataTest extends AbstractMainTest {
    
    @Test(timeout=10000)
    public void testStartAndStop() throws Exception {
        final long pause = 1000L;
        Component<?> root = Modules.newRootComponent();
        Component<?> server = ControlModules.newControlSingletonEnsemble(root);
        Component<?> client = ControlModules.newControlClient(
                ImmutableList.of(root, server),
                ImmutableList.of(Module.create()),
                ControlModules.ControlClientProvider.class);
        pauseWithComponents(ImmutableList.of(root, server, client), pause);
    }
}
