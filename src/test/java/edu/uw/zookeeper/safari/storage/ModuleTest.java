package edu.uw.zookeeper.safari.storage;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.name.Named;

import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Modules;

public class ModuleTest extends AbstractMainTest {

    @Test(timeout=10000)
    public void testStartAndStop() throws Exception {
        final long pause = 1000L;
        Component<Named> root = Modules.newRootComponent();
        Component<?> server = StorageModules.newStorageSingletonEnsemble(root);
        Component<?> client = StorageModules.newStorageSingletonClient(
                server, ImmutableList.of(root));
        pauseWithComponents(
                ImmutableList.<Component<?>>of(root, server, client), 
                pause);
    }
}
