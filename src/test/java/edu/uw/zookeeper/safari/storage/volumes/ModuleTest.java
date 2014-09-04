package edu.uw.zookeeper.safari.storage.volumes;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.storage.StorageModules;

@RunWith(JUnit4.class)
public class ModuleTest extends AbstractMainTest {
    
    @Test(timeout=10000)
    public void testStartAndStop() throws Exception {
        final long pause = 1000L;
        Component<?> root = Modules.newRootComponent();
        Component<?> server = StorageModules.newStorageSingletonEnsemble(root);
        Component<?> client = StorageModules.newStorageSingletonClient(
                server, ImmutableList.of(root), ImmutableList.of(Module.create()));
        pauseWithComponents(ImmutableList.of(root, server, client), pause);
    }
}
