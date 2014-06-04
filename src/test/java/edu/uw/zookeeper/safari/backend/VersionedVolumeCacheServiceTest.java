package edu.uw.zookeeper.safari.backend;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;

import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.region.Region;

@RunWith(JUnit4.class)
public class VersionedVolumeCacheServiceTest extends AbstractMainTest {
    
    public static class VersionedVolumeCacheServiceTestModule extends Module {
        
        public static VersionedVolumeCacheServiceTestModule forRegion(
                final Identifier region) {
            return new VersionedVolumeCacheServiceTestModule(
                    new AbstractModule() {
                        @Override
                        protected void configure() {
                            bind(Identifier.class).annotatedWith(Region.class).toInstance(region);
                        }
                    });
        }
        
        protected final com.google.inject.Module module;
        
        protected VersionedVolumeCacheServiceTestModule(
                com.google.inject.Module module) {
            this.module = module;
        }

        @Override    
        protected ImmutableList<? extends com.google.inject.Module> getModules() {
            return ImmutableList.<com.google.inject.Module>of(
                    module,
                    VersionedVolumeCacheService.module());
        }

        @Override  
        protected Class<? extends Service> getServiceType() {
            return VersionedVolumeCacheService.class;
        }
    }

    @Test(timeout=15000)
    public void testStartAndStop() throws Exception {
        final long pause = 3000L;
        final Identifier region = Identifier.valueOf(1);
        final Component<?> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        final Component<?> client = ControlModules.newControlClient(
                ImmutableList.of(root, control),
                ImmutableList.of(
                        edu.uw.zookeeper.safari.data.Module.create(),
                        VersionedVolumeCacheServiceTestModule.forRegion(region)),
                ControlModules.ControlClientProvider.class);
        // TODO
        pauseWithComponents(
                ImmutableList.<Component<?>>of(root, control, client), 
                pause);
    }
}
