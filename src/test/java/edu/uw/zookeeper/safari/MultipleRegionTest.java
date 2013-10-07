package edu.uw.zookeeper.safari;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.inject.Guice;
import com.google.inject.Injector;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.SingleClientTest.SingleClientService;
import edu.uw.zookeeper.safari.common.GuiceRuntimeModule;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlTest;
import edu.uw.zookeeper.safari.control.SimpleControlConnectionsService;
import edu.uw.zookeeper.safari.data.Volume;
import edu.uw.zookeeper.safari.data.VolumeCacheService;
import edu.uw.zookeeper.safari.data.VolumeDescriptor;
import edu.uw.zookeeper.safari.frontend.AssignmentCacheService;
import edu.uw.zookeeper.safari.net.IntraVmAsNetModule;
import edu.uw.zookeeper.safari.peer.EnsembleConfiguration;

@RunWith(JUnit4.class)
public class MultipleRegionTest {

    public static Injector injector() {
        return Guice.createInjector(
                GuiceRuntimeModule.create(DefaultRuntimeModule.defaults()),
                IntraVmAsNetModule.create(),
                SimpleControlConnectionsService.module());
    }
    
    public static Injector injector(Injector parent) {
        return SingleClientTest.SingleClientService.Module.injector(
                BootstrapTest.SimpleMainService.Module.injector(
                BootstrapTest.injector(
                        parent.createChildInjector(ControlTest.module()))));
    }

    @Test(timeout=10000)
    public void test() throws Exception {
        Injector rootInjector = injector();
        int num_regions = 2;
        
        Injector controlInjector = rootInjector.createChildInjector(ControlTest.module());
        ControlMaterializerService control = controlInjector.getInstance(ControlMaterializerService.class);
        control.startAsync().awaitRunning();
        
        // Create root volume and reserve
        Volume rootVolume = Volume.of(
                ControlSchema.Volumes.Entity.create(VolumeDescriptor.all(), control.materializer()).get().get(),
                VolumeDescriptor.all());
        assertEquals(Identifier.zero(), 
                ControlSchema.Volumes.Entity.Region.create(
                    Identifier.zero(), 
                    ControlSchema.Volumes.Entity.of(rootVolume.getId()), 
                    control.materializer()).get().get());
        
        Injector[] regionInjectors = new Injector[num_regions];
        for (int i=0; i<num_regions; ++i){ 
            regionInjectors[i] = injector(rootInjector);
        }
        for (Injector injector: regionInjectors) {
            injector.getInstance(BootstrapTest.SimpleMainService.class).startAsync();
        }
        for (Injector injector: regionInjectors) {
            injector.getInstance(BootstrapTest.SimpleMainService.class).awaitRunning();
        }
        
        ServiceMonitor monitor = rootInjector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        
        // Create and assign a volume for each region
        Volume[] volumes = new Volume[num_regions];
        for (int i=0; i<num_regions; ++i){ 
            Identifier id = regionInjectors[i].getInstance(EnsembleConfiguration.class).getEnsemble();
            VolumeDescriptor vd = VolumeDescriptor.of(((ZNodeLabel.Path) ZNodeLabel.joined(ZNodeLabel.Path.root(), id.toString())));

            // Add new leaf to root volume
            rootVolume = Volume.of(
                    rootVolume.getId(), 
                    rootVolume.getDescriptor().add(vd.getRoot()));
            assertEquals(rootVolume.getDescriptor(),
                    ControlSchema.Volumes.Entity.Volume.set(
                            rootVolume.getDescriptor(), 
                            ControlSchema.Volumes.Entity.of(rootVolume.getId()), control.materializer()).get().get());
            
            // Create volume
            volumes[i] = Volume.of(
                    ControlSchema.Volumes.Entity.create(vd, control.materializer()).get().get(),
                    vd);
            
            // Assign volume
            assertEquals(id,
                    ControlSchema.Volumes.Entity.Region.create(
                            id, ControlSchema.Volumes.Entity.of(volumes[i].getId()), 
                            control.materializer()).get().get());
        }
        
        // Assign root to region 0
        Identifier id = regionInjectors[0].getInstance(EnsembleConfiguration.class).getEnsemble();
        assertEquals(id, ControlSchema.Volumes.Entity.Region.set(
                id, ControlSchema.Volumes.Entity.of(rootVolume.getId()), 
                control.materializer()).get().get());
        
        // Clear caches
        for (Injector injector: regionInjectors) {
            injector.getInstance(VolumeCacheService.class).cache().clear();
            injector.getInstance(AssignmentCacheService.class).get().asCache().clear();
        }

        SingleClientService[] clients = new SingleClientService[regionInjectors.length];
        for (int i=0; i<num_regions; ++i){ 
            clients[i] = regionInjectors[i].getInstance(SingleClientService.class);
            clients[i].startAsync().awaitRunning();
        }
        
        // create root
        Message.ServerResponse<?> response = clients[0].getClient().getConnectionClientExecutor().submit(
                Operations.Requests.create().setPath(ZNodeLabel.Path.root()).build()).get();
        assertFalse(response instanceof Operation.Error);
        
        // create all volume roots
        for (Volume v: volumes) {
            response = clients[0].getClient().getConnectionClientExecutor().submit(
                    Operations.Requests.create().setPath(v.getDescriptor().getRoot()).build()).get();
            assertFalse(response instanceof Operation.Error);
        }

        monitor.stopAsync().awaitTerminated();
    }
}
