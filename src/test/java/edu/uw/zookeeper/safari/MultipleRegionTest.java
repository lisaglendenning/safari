package edu.uw.zookeeper.safari;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.SubmitGenerator;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.CountingGenerator;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Generators;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.control.volumes.PrepareVolumeOperation;
import edu.uw.zookeeper.safari.control.volumes.VolumeEntryResponse;
import edu.uw.zookeeper.safari.control.volumes.VolumeOperationCoordinatorEntry;
import edu.uw.zookeeper.safari.control.volumes.VolumesSchemaRequests;
import edu.uw.zookeeper.safari.frontend.Frontend;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.schema.volumes.AssignParameters;
import edu.uw.zookeeper.safari.schema.volumes.AssignedVolumeBranches;
import edu.uw.zookeeper.safari.schema.volumes.EmptyVolume;
import edu.uw.zookeeper.safari.schema.volumes.MergeParameters;
import edu.uw.zookeeper.safari.schema.volumes.SplitParameters;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperator;
import edu.uw.zookeeper.safari.schema.volumes.VolumeVersion;
import edu.uw.zookeeper.safari.storage.volumes.VolumesLeaseManager;

@RunWith(JUnit4.class)
public class MultipleRegionTest extends AbstractMainTest {

    @Test(timeout=35000)
    public void testStartAndStop() throws Exception {
        final long pause = 5000L;
        final int num_regions = 2;
        pauseWithComponents(SafariModules.newSingletonSafariRegions(num_regions), pause);
    }

    @Test(timeout=45000)
    public void testClientConnect() throws Exception {
        final long pause = 5000L;
        final int num_regions = 2;
        
        List<Component<?>> components = SafariModules.newSingletonSafariRegions(num_regions);
        for (int i=1; i<components.size(); ++i) {
            Services.startAndWait(components.get(i).injector().getInstance(Service.class));
        }
        
        // connect client to last region
        final Component<?> client = Modules.newClientComponent(
                components.get(0), 
                EnsembleView.copyOf(components.get(components.size()-1).injector().getInstance(Key.get(ServerInetAddressView.class, Frontend.class))),
                Names.named("client"));
        
        components = ImmutableList.<Component<?>>builder().addAll(components).add(client).build();
        pauseWithComponents(components, pause);
    }

    @Test(timeout=60000)
    public void testClientPipeline() throws Exception {
        final int iterations = 32;
        final int logInterval = 8;
        final int num_regions = 2;
        final TimeValue timeOut = TimeValue.seconds(15L);

        List<Component<?>> components = SafariModules.newSingletonSafariRegions(num_regions);
        for (int i=1; i<components.size(); ++i) {
            Services.startAndWait(components.get(i).injector().getInstance(Service.class));
        }
        
        // connect client to last region
        final Component<?> client = Modules.newClientComponent(
                components.get(0), 
                EnsembleView.copyOf(components.get(components.size()-1).injector().getInstance(Key.get(ServerInetAddressView.class, Frontend.class))),
                Names.named("client"));
        
        components = ImmutableList.<Component<?>>builder().addAll(components).add(client).build();
        
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                final Queue<ListenableFuture<Message.ServerResponse<?>>> futures = Queues.newArrayDeque();
                final Generator<? extends Records.Request> requests = Generators.constant(Operations.Requests.exists().setPath(ZNodePath.root()).build());
                final CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = 
                        CountingGenerator.create(iterations, logInterval, 
                                SubmitGenerator.create(requests, client.injector().getInstance(ConnectionClientExecutorService.Builder.class).getConnectionClientExecutor()), logger);
                while (operations.hasNext()) {
                     futures.add(operations.next().second());
                     if ((operations.getCount() == operations.getLimit()) || (operations.getCount() % logInterval == 0)) {
                         ListenableFuture<Message.ServerResponse<?>> response;
                         while ((response = futures.poll()) != null) {
                             assertFalse(response.get(timeOut.value(), timeOut.unit()).record() instanceof Operation.Error);
                         }
                     }
                }
                assertTrue(futures.isEmpty());
                return null;
            }
        };
        callWithService(
                stopping(components),
                callable);
    }

    @Test(timeout=60000)
    public void testTransfer() throws Exception {
        final int num_regions = 2;
        final long pause = 6000L;
        
        final List<Component<?>> components = SafariModules.newSingletonSafariRegions(num_regions);
        final List<Component<?>> regions = Lists.newArrayListWithCapacity(num_regions);
        for (int i=1; i<components.size(); ++i) {
            Component<?> component = components.get(i);
            if ((component.annotation() instanceof Named) && (((Named) component.annotation()).value().startsWith("region-"))) {
                VolumesLeaseManager.ConfigurableLeaseDuration.set(component.injector().getInstance(Configuration.class), TimeValue.seconds(5L));
                regions.add(component);
            }
        }
        final Injector injector = stopping(components);
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                AssignedVolumeBranches volume = regions.get(0).injector().getInstance(Key.get(new TypeLiteral<AsyncFunction<ZNodePath, AssignedVolumeBranches>>(){}, edu.uw.zookeeper.safari.control.volumes.Volumes.class)).apply(ZNodePath.root()).get();
                
                Component<?> fromRegion = null;
                Component<?> toRegion = null;
                for (Component<?> component: regions) {
                    Identifier region = component.injector().getInstance(Key.get(Identifier.class, edu.uw.zookeeper.safari.region.Region.class));
                    if (region.equals(volume.getState().getValue().getRegion())) {
                        fromRegion = component;
                    } else if (toRegion == null) {
                        toRegion = component;
                        break;
                    }
                }
                assertNotNull(toRegion);
                
                SchemaClientService<ControlZNode<?>,?> control = fromRegion.injector().getInstance(
                        Key.get(new TypeLiteral<SchemaClientService<ControlZNode<?>,?>>(){}));
                VolumeOperation<?> operation = PrepareVolumeOperation.create(
                        VolumesSchemaRequests.create(control.materializer())
                                        .volume(volume.getDescriptor().getId()), 
                        VolumeOperator.TRANSFER, 
                        ImmutableList.of(toRegion.injector().getInstance(Key.get(Identifier.class, edu.uw.zookeeper.safari.region.Region.class)))).get();
                commit(operation, control, regions);
                
                pause(pause);
                return null;
            }
        };
        callWithService(
                injector,
                callable);
    }

    @Test(timeout=120000)
    public void testSplitMerge() throws Exception {
        final int num_regions = 2;
        final long pause = 6000L;
        
        List<Component<?>> components = SafariModules.newSingletonSafariRegions(num_regions);
        final List<Component<?>> regions = Lists.newArrayListWithCapacity(num_regions);
        for (int i=1; i<components.size(); ++i) {
            Component<?> component = components.get(i);
            if ((component.annotation() instanceof Named) && (((Named) component.annotation()).value().startsWith("region-"))) {
                VolumesLeaseManager.ConfigurableLeaseDuration.set(component.injector().getInstance(Configuration.class), TimeValue.seconds(5L));
                regions.add(component);
            }
            Services.startAndWait(component.injector().getInstance(Service.class));
        }
        
        // connect client to first region
        final Component<?> client = Modules.newClientComponent(
                components.get(0), 
                EnsembleView.copyOf(regions.get(0).injector().getInstance(Key.get(ServerInetAddressView.class, Frontend.class))),
                Names.named("client"));
        components = ImmutableList.<Component<?>>builder().addAll(components).add(client).build();
        
        final Injector injector = stopping(components);
        
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                AssignedVolumeBranches volume = regions.get(0).injector().getInstance(Key.get(new TypeLiteral<AsyncFunction<ZNodePath, AssignedVolumeBranches>>(){}, edu.uw.zookeeper.safari.control.volumes.Volumes.class)).apply(ZNodePath.root()).get();
                
                final ZNodeLabel branch = ZNodeLabel.fromString("1");
                Operations.unlessError(client.injector().getInstance(ConnectionClientExecutorService.Builder.class).getConnectionClientExecutor().submit(Operations.Requests.create().setPath(ZNodePath.root().join(branch)).build()).get().record());
                
                Component<?> fromRegion = null;
                Component<?> toRegion = null;
                for (Component<?> component: regions) {
                    Identifier region = component.injector().getInstance(Key.get(Identifier.class, edu.uw.zookeeper.safari.region.Region.class));
                    if (region.equals(volume.getState().getValue().getRegion())) {
                        fromRegion = component;
                    } else if (toRegion == null) {
                        toRegion = component;
                        break;
                    }
                }
                assertNotNull(toRegion);
                
                SchemaClientService<ControlZNode<?>,?> control = fromRegion.injector().getInstance(
                        Key.get(new TypeLiteral<SchemaClientService<ControlZNode<?>,?>>(){}));
                VolumeOperation<?> operation = PrepareVolumeOperation.create(
                        VolumesSchemaRequests.create(control.materializer())
                                        .volume(volume.getDescriptor().getId()), 
                        VolumeOperator.SPLIT, 
                        ImmutableList.of(
                                toRegion.injector().getInstance(Key.get(Identifier.class, edu.uw.zookeeper.safari.region.Region.class)),
                                branch)).get();
                Identifier leaf = ((SplitParameters) operation.getOperator().getParameters()).getLeaf();
                logger.info("Split leaf is {}", leaf);
                commit(operation, control, regions);
                
                operation = PrepareVolumeOperation.create(
                        VolumesSchemaRequests.create(control.materializer())
                                        .volume(leaf), 
                        VolumeOperator.MERGE, 
                        ImmutableList.of()).get();
                commit(operation, control, regions);
                
                pause(pause);
                return null;
            }
        };
        callWithService(
                injector,
                callable);
    }
    
    protected void committed(VolumeLogEntryPath entry, SchemaClientService<ControlZNode<?>,?> control) throws InterruptedException, ExecutionException {
        assertTrue(VolumeEntryResponse.voted(entry, control.materializer(), control.cacheEvents(), logger).get().booleanValue());
        logger.info("{} voted", entry);
        assertTrue(VolumeEntryResponse.committed(entry, control.materializer(), control.cacheEvents(), logger).get().booleanValue());
        logger.info("{} committed", entry);
    }
    
    protected void commit(VolumeOperation<?> operation, SchemaClientService<ControlZNode<?>,?> control, Iterable<? extends Component<?>> regions) throws Exception {
        logger.info("Proposing {}", operation);
        VolumeLogEntryPath entry = VolumeOperationCoordinatorEntry.newEntry(operation, control.materializer()).get();
        logger.info("Proposed {}", entry);
        committed(entry, control);
        for (Component<?> component: regions) {
            List<Identifier> ids;
            switch (operation.getOperator().getOperator()) {
            case MERGE:
            {
                ids = ImmutableList.of(operation.getVolume().getValue(), ((MergeParameters) operation.getOperator().getParameters()).getParent().getValue());
                break;
            }
            case SPLIT:
            {
                ids = ImmutableList.of(operation.getVolume().getValue(), ((SplitParameters) operation.getOperator().getParameters()).getLeaf());
                break;
            }
            case TRANSFER:
            {
                ids = ImmutableList.of(operation.getVolume().getValue());
                break;
            }
            default:
                throw new AssertionError();
            }
            for (Identifier id: ids) {
                UnsignedLong version;
                do {
                    FutureTransition<UnsignedLong> latest = component.injector().getInstance(Key.get(new TypeLiteral<Function<Identifier, FutureTransition<UnsignedLong>>>(){}, edu.uw.zookeeper.safari.control.volumes.Volumes.class)).apply(id);
                    version = latest.getCurrent().orNull();
                    if (version == null) {
                        version = latest.getNext().get();
                    }
                } while (!version.equals(operation.getOperator().getParameters().getVersion()));
                AsyncFunction<VersionedId, VolumeVersion<?>> versions = component.injector().getInstance(Key.get(new TypeLiteral<AsyncFunction<VersionedId, VolumeVersion<?>>>(){}, edu.uw.zookeeper.safari.control.volumes.Volumes.class));
                VolumeVersion<?> volume = versions.apply(VersionedId.valueOf(version, id)).get();
                switch (operation.getOperator().getOperator()) {
                case MERGE:
                {
                    if (id == ids.get(0)) {
                        assertTrue(volume instanceof EmptyVolume);
                    } else {
                        assertEquals(((AssignedVolumeBranches) versions.apply(((MergeParameters) operation.getOperator().getParameters()).getParent()).get()).getState().getValue().getRegion(), ((AssignedVolumeBranches) volume).getState().getValue().getRegion());
                        assertFalse(((AssignedVolumeBranches) volume).getState().getValue().getLeaves().contains(ids.get(0)));
                    }
                    break;
                }
                case SPLIT:
                {
                    if (id == ids.get(0)) {
                        AssignedVolumeBranches prev = (AssignedVolumeBranches) versions.apply(operation.getVolume()).get();
                        assertEquals(prev.getState().getValue().getRegion(), ((AssignedVolumeBranches) volume).getState().getValue().getRegion());
                        assertTrue(((AssignedVolumeBranches) volume).getState().getValue().getLeaves().contains(ids.get(1)));
                    } else {
                        assertEquals(((SplitParameters) operation.getOperator().getParameters()).getRegion(), ((AssignedVolumeBranches) volume).getState().getValue().getRegion());
                    }
                    break;
                }
                case TRANSFER:
                {
                    assertEquals(((AssignParameters) operation.getOperator().getParameters()).getRegion(), ((AssignedVolumeBranches) volume).getState().getValue().getRegion());
                    assertEquals(((AssignedVolumeBranches) versions.apply(operation.getVolume()).get()).getState().getValue().getBranches(), ((AssignedVolumeBranches) volume).getState().getValue().getBranches());
                    break;
                }
                default:
                    throw new AssertionError();
                }
            }
        }
    }
}
