package edu.uw.zookeeper.safari.control.volumes;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.safari.AbstractMainTest;
import edu.uw.zookeeper.safari.Component;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.Modules;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.volumes.LatestVolumeCache;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.EmptyVolume;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;

@RunWith(JUnit4.class)
public class LatestVolumeCacheTest extends AbstractMainTest {

    public static Module module() {
        VolumeWatchers module = VolumeWatchers.create();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                    VolumeDescriptorCache.module(),
                    VolumeBranchesCache.module(),
                    LatestVolumeCache.module(),
                    module));
    }
    
    @Test(timeout=20000)
    public void test() throws Exception {
        final Component<?> root = Modules.newRootComponent();
        final Component<?> server = ControlModules.newControlSingletonEnsemble(root);
        final Component<?> client = ControlModules.newControlClient(
                ImmutableList.of(root, server),
                ImmutableList.of(module()),
                ControlModules.ControlClientProvider.class);
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Thread.sleep(1000L);
                
                final long timeout = 3000L;
                final Identifier region = Identifier.valueOf(1);
                final VolumeDescriptorCache descriptors = client.injector().getInstance(VolumeDescriptorCache.class);
                final LatestVolumeCache volumes = client.injector().getInstance(LatestVolumeCache.class);
                final SchemaClientService<ControlZNode<?>,?> control = client.injector().getInstance(Key.get(new TypeLiteral<SchemaClientService<ControlZNode<?>,?>>(){}));
                final SimpleVolumeOperator operator = client.injector().getInstance(SimpleVolumeOperator.class);
                
                // create root volume
                ZNodePath path = ZNodePath.root();
                AssignedVolumeBranches parent = addVolume(
                        descriptors,
                        volumes,
                        control,
                        operator,
                        path, 
                        region, 
                        timeout);

                // create child volume
                path = parent.getDescriptor().getPath().join(ZNodeLabel.fromString("1"));
                AssignedVolumeBranches child = addVolume(
                        descriptors,
                        volumes,
                        control,
                        operator,
                        path, 
                        region, 
                        timeout);
                
                // delete child volume
                removeVolume(
                        descriptors,
                        volumes,
                        control,
                        operator,
                        child, 
                        timeout);
                
                return null;
            }
        };
        callWithService(
                stopping(ImmutableList.of(root, server, client)),
                callable);
    }
    
    protected AssignedVolumeBranches addVolume(
            final VolumeDescriptorCache descriptors, 
            final LatestVolumeCache volumes, 
            final SchemaClientService<ControlZNode<?>,?> control, 
            final SimpleVolumeOperator operator, 
            final ZNodePath path, 
            final Identifier region, 
            final long timeout) throws Exception {
        Optional<AssignedVolumeBranches> parent = Optional.absent();
        ListenableFuture<AssignedVolumeBranches> byPath = volumes.pathToVolume().apply(path);
        if (volumes.volumes().isEmpty()) {
            assertFalse(byPath.isDone());
        } else if (byPath.isDone()) {
            parent = Optional.of(byPath.get());
        }
        VolumesSchemaRequests<?> requests = VolumesSchemaRequests.create(control.materializer());
        UnsignedLong version = UnsignedLong.valueOf(System.currentTimeMillis());
        AssignedVolumeBranches volume = (AssignedVolumeBranches) operator.difference(
                    path,
                    version,
                    region).get(timeout, TimeUnit.MILLISECONDS);
        assertEquals(descriptors.descriptors().lookup().apply(volume.getDescriptor().getId()).get(0L, TimeUnit.MILLISECONDS), path);
        ListenableFuture<VolumeVersion<?>> byId = volumes.idToVolume().apply(volume.getDescriptor().getId());
        assertFalse(byId.isDone());
        Operations.unlessError(
                control.materializer().submit(new IMultiRequest(ImmutableList.of(requests.volume(volume.getDescriptor().getId()).version(version).latest().create())))
                .get(timeout, TimeUnit.MILLISECONDS).record());
        byPath = volumes.pathToVolume().apply(path);
        assertEquals(volume, byPath.get(timeout, TimeUnit.MILLISECONDS));
        assertEquals(byId.get(0L, TimeUnit.MILLISECONDS), volume);
        logger.info("Added {}", volume);
        
        if (parent.isPresent()) {
            Operations.unlessError(
                    control.materializer().submit(new IMultiRequest(ImmutableList.of(requests.volume(parent.get().getDescriptor().getId()).version(version).latest().update())))
                    .get(timeout, TimeUnit.MILLISECONDS).record());
            byPath = volumes.pathToVolume().apply(((AbsoluteZNodePath) path).parent());
            assertEquals(parent.get().getDescriptor(), byPath.get(timeout, TimeUnit.MILLISECONDS).getDescriptor());
            assertEquals(version, byPath.get(0L, TimeUnit.MILLISECONDS).getState().getVersion());
            assertTrue(byPath.get(0L, TimeUnit.MILLISECONDS).getState().getValue().getBranches().inverse().containsKey(volume.getDescriptor().getId()));
            byId = volumes.idToVolume().apply(parent.get().getDescriptor().getId());
            assertEquals(byPath.get(0L, TimeUnit.MILLISECONDS), byId.get(0L, TimeUnit.MILLISECONDS));

            byPath = volumes.pathToVolume().apply(path);
            assertEquals(volume, byPath.get(0L, TimeUnit.MILLISECONDS));
        }
        
        return volume;
    }
    
    protected EmptyVolume removeVolume(
            final VolumeDescriptorCache descriptors, 
            final LatestVolumeCache volumes, 
            final SchemaClientService<ControlZNode<?>,?> control, 
            final SimpleVolumeOperator operator, 
            final AssignedVolumeBranches volume, 
            final long timeout) throws Exception {
        AssignedVolumeBranches parent = volumes.pathToVolume().apply(((AbsoluteZNodePath) volume.getDescriptor().getPath()).parent()).get(timeout, TimeUnit.MILLISECONDS);
        UnsignedLong version = UnsignedLong.valueOf(System.currentTimeMillis());
        VolumesSchemaRequests<?> requests = VolumesSchemaRequests.create(control.materializer());
        operator.union(volume, version).get(timeout, TimeUnit.MILLISECONDS);
        Operations.unlessError(
                control.materializer().submit(new IMultiRequest(ImmutableList.of(requests.volume(volume.getDescriptor().getId()).version(version).latest().update())))
                .get(timeout, TimeUnit.MILLISECONDS).record());
        ListenableFuture<AssignedVolumeBranches> byPath = volumes.pathToVolume().apply(volume.getDescriptor().getPath());
        assertFalse(byPath.isDone());
        ListenableFuture<VolumeVersion<?>> byId = volumes.idToVolume().apply(volume.getDescriptor().getId());
        EmptyVolume deleted = (EmptyVolume) byId.get(0L, TimeUnit.MILLISECONDS);
        assertEquals(EmptyVolume.valueOf(volume.getDescriptor(), version), deleted);
        assertEquals(descriptors.descriptors().lookup().apply(volume.getDescriptor().getId()).get(0L, TimeUnit.MILLISECONDS), volume.getDescriptor().getPath());
        Operations.unlessError(
                control.materializer().submit(new IMultiRequest(ImmutableList.of(requests.volume(parent.getDescriptor().getId()).version(version).latest().update())))
                .get(timeout, TimeUnit.MILLISECONDS).record());
        assertEquals(parent.getDescriptor(), byPath.get(timeout, TimeUnit.MILLISECONDS).getDescriptor());
        assertEquals(version, byPath.get(0L, TimeUnit.MILLISECONDS).getState().getVersion());
        assertFalse(byPath.get(0L, TimeUnit.MILLISECONDS).getState().getValue().getBranches().inverse().containsKey(volume.getDescriptor().getId()));
        logger.info("Removed {}", deleted);
        
        return deleted;
    }
}
