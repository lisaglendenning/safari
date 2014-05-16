package edu.uw.zookeeper.safari.control;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.inject.name.Named;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;

@RunWith(JUnit4.class)
public class ControlTest extends AbstractMainTest {
    
    @Test(timeout=10000)
    public void testStartAndStop() throws Exception {
        final long pause = 1000L;
        Component<Named> root = Modules.newRootComponent();
        Component<?> server = ControlModules.newControlSingletonEnsemble(root);
        Component<?> client = ControlModules.newControlClient(
                ImmutableList.of(root, server));
        pauseWithComponents(ImmutableList.of(root, server, client), pause);
    }
}
