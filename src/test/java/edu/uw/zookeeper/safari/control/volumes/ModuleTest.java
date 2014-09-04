package edu.uw.zookeeper.safari.control.volumes;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.region.RegionModules;
import edu.uw.zookeeper.safari.storage.StorageModules;

@RunWith(JUnit4.class)
public class ModuleTest extends AbstractMainTest {
    
    @Test(timeout=15000)
    public void testStartAndStop() throws Exception {
        final long pause = 5000L;
        final Component<?> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        final Component<?> server = StorageModules.newStorageSingletonEnsemble(root);
        final Component<?> client = RegionModules.newSingletonRegionMember(
                server, ImmutableList.of(root, control), 
                ImmutableList.<SafariModule>of(Module.create()));
        pauseWithComponents(
                ImmutableList.<Component<?>>of(root, control, server, client), 
                pause);
    }
}
