package edu.uw.zookeeper.safari.data;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.control.ControlSchema;

@RunWith(JUnit4.class)
public class VolumeCacheServiceTest extends AbstractMainTest {
    
    @Test(timeout=30000)
    public void test() throws Exception {
        final Component<?> root = Modules.newRootComponent();
        final Component<?> server = ControlModules.newControlSingletonEnsemble(root);
        final Component<?> client = ControlModules.newControlClient(
                ImmutableList.of(root, server),
                ImmutableList.of(Module.create()),
                ControlModules.ControlClientProvider.class);
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Thread.sleep(1000L);
                
                final long timeout = 3000L;
                final Identifier region = Identifier.valueOf(1);
                final VolumeCacheService volumes = client.injector().getInstance(VolumeCacheService.class);
                final ControlClientService control = client.injector().getInstance(ControlClientService.class);
                final VolumeOperator operator = client.injector().getInstance(VolumeOperator.class);
                
                // create root volume
                ZNodePath path = ZNodePath.root();
                Volume parent = addVolume(
                        volumes,
                        control,
                        operator,
                        path, 
                        region, 
                        timeout);

                // create child volume
                path = parent.getDescriptor().getPath().join(ZNodeLabel.fromString("1"));
                Volume child = addVolume(
                        volumes,
                        control,
                        operator,
                        path, 
                        region, 
                        timeout);
                
                // delete child volume
                removeVolume(
                        volumes,
                        control,
                        operator,
                        child, 
                        timeout);
                
                return null;
            }
        };
        callWithService(
                monitored(ImmutableList.of(root, server, client), Modules.StoppingServiceMonitorProvider.class),
                callable);
    }
    
    protected Volume addVolume(
            final VolumeCacheService volumes, final ControlClientService control, final VolumeOperator operator, final ZNodePath path, final Identifier region, final long timeout) throws Exception {
        Optional<Volume> parent = Optional.absent();
        ListenableFuture<Volume> byPath = volumes.pathToVolume().apply(path);
        if (volumes.volumes().isEmpty()) {
            assertFalse(byPath.isDone());
        } else if (byPath.isDone()) {
            parent = Optional.of(byPath.get());
        }
        VolumeDescriptor descriptor = VolumeDescriptor.valueOf(
                operator.create(path).get(timeout, TimeUnit.MILLISECONDS), 
                path);
        assertEquals(volumes.idToPath().asLookup().apply(descriptor.getId()).get(0L, TimeUnit.MILLISECONDS), path);
        ListenableFuture<VersionedVolume> byId = volumes.idToVolume().apply(descriptor.getId());
        assertFalse(byId.isDone());
        UnsignedLong version = UnsignedLong.valueOf(System.currentTimeMillis());
        Operations.unlessError(
                operator.difference(
                    descriptor,
                    version,
                    region).get(timeout, TimeUnit.MILLISECONDS).record());
        Operations.unlessError(
                control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(descriptor.getId()), version)
                .call().get(timeout, TimeUnit.MILLISECONDS).record());
        byPath = volumes.pathToVolume().apply(path);
        Volume volume = byPath.get(timeout, TimeUnit.MILLISECONDS);
        assertEquals(Volume.valueOf(descriptor, version, region, ImmutableMap.<ZNodeName, Identifier>of()), volume);
        assertEquals(byId.get(0L, TimeUnit.MILLISECONDS), volume);
        logger.info("Added {}", volume);
        
        if (parent.isPresent()) {
            Operations.unlessError(
                    control.materializer().setData(ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(parent.get().getDescriptor().getId()), version)
                    .call().get(timeout, TimeUnit.MILLISECONDS).record());
            byPath = volumes.pathToVolume().apply(((AbsoluteZNodePath) path).parent());
            assertEquals(parent.get().getDescriptor(), byPath.get(timeout, TimeUnit.MILLISECONDS).getDescriptor());
            assertEquals(version, byPath.get(0L, TimeUnit.MILLISECONDS).getVersion());
            assertTrue(byPath.get(0L, TimeUnit.MILLISECONDS).getBranches().inverse().containsKey(volume.getDescriptor().getId()));
            byId = volumes.idToVolume().apply(parent.get().getDescriptor().getId());
            assertEquals(byPath.get(0L, TimeUnit.MILLISECONDS), byId.get(0L, TimeUnit.MILLISECONDS));

            byPath = volumes.pathToVolume().apply(path);
            assertEquals(volume, byPath.get(0L, TimeUnit.MILLISECONDS));
        }
        
        return volume;
    }
    
    protected EmptyVolume removeVolume(
            final VolumeCacheService volumes, final ControlClientService control, final VolumeOperator operator, final Volume volume, final long timeout) throws Exception {
        Volume parent = volumes.pathToVolume().apply(((AbsoluteZNodePath) volume.getDescriptor().getPath()).parent()).get(timeout, TimeUnit.MILLISECONDS);
        UnsignedLong version = UnsignedLong.valueOf(System.currentTimeMillis());
        Operations.unlessError(
                operator.union(volume, version).get(timeout, TimeUnit.MILLISECONDS).record());
        Operations.unlessError(
                control.materializer().setData(ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(volume.getDescriptor().getId()), version)
                .call().get(timeout, TimeUnit.MILLISECONDS).record());
        ListenableFuture<Volume> byPath = volumes.pathToVolume().apply(volume.getDescriptor().getPath());
        assertFalse(byPath.isDone());
        ListenableFuture<VersionedVolume> byId = volumes.idToVolume().apply(volume.getDescriptor().getId());
        EmptyVolume deleted = (EmptyVolume) byId.get(0L, TimeUnit.MILLISECONDS);
        assertEquals(EmptyVolume.valueOf(volume.getDescriptor(), version), deleted);
        assertEquals(volumes.idToPath().asLookup().apply(volume.getDescriptor().getId()).get(0L, TimeUnit.MILLISECONDS), volume.getDescriptor().getPath());
        Operations.unlessError(
                control.materializer().setData(ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(parent.getDescriptor().getId()), version)
                .call().get(timeout, TimeUnit.MILLISECONDS).record());
        assertEquals(parent.getDescriptor(), byPath.get(timeout, TimeUnit.MILLISECONDS).getDescriptor());
        assertEquals(version, byPath.get(0L, TimeUnit.MILLISECONDS).getVersion());
        assertFalse(byPath.get(0L, TimeUnit.MILLISECONDS).getBranches().inverse().containsKey(volume.getDescriptor().getId()));
        logger.info("Removed {}", deleted);
        
        return deleted;
    }
}
