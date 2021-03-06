package edu.uw.zookeeper.safari.peer;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.inject.name.Named;

import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.control.ControlModules;

@RunWith(JUnit4.class)
public class ModuleTest extends AbstractMainTest {

    @Test(timeout=10000)
    public void testStartAndStop() throws Exception {
        final long pause = 1000L;
        Component<Named> root = Modules.newRootComponent();
        Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        Component<?> peer = PeerModules.newPeer(
                ImmutableList.of(root, control));
        pauseWithComponents(ImmutableList.of(root, control, peer), pause);
    }
}
