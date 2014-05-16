package edu.uw.zookeeper.safari;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.Service;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.VolumeDescriptor;

@RunWith(JUnit4.class)
public class MultipleRegionTest extends AbstractMainTest {

    @Ignore
    @Test(timeout=30000)
    public void test() throws Exception {
        final int num_regions = 2;

        final Component<?> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        Services.startAndWait(control.injector().getInstance(Service.class));
        
        // Create root volume
        VolumeDescriptor rootVd = VolumeDescriptor.valueOf(
                ControlZNode.CreateEntity.call(
                        ControlSchema.Safari.Volumes.PATH, 
                        ZNodePath.root(), 
                        control.injector().getInstance(ControlClientService.class).materializer()).get(),
                ZNodePath.root());
        // TODO reserve
        /*
        // Create storage
        List<Component<?>>[] storage = new List<Component<?>>[num_regions];
        Injector[] regionInjectors = new Injector[num_regions];
        for (int i=0; i<num_regions; ++i) { 
            regionInjectors[i] = injector(rootInjector);
        }
        for (Injector injector: regionInjectors) {
            injector.getInstance(SingletonTest.SimpleMainService.class).startAsync();
        }
        for (Injector injector: regionInjectors) {
            injector.getInstance(SingletonTest.SimpleMainService.class).awaitRunning();
        }
        
        
        ServiceMonitor monitor = rootInjector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();

        // Create and assign a volume for each region
        UnsignedLong version = UnsignedLong.valueOf(1L);
        List<Volume> volumes = Lists.newArrayListWithCapacity(num_regions);
        for (int i=0; i<num_regions; ++i) { 

            // TODO create volume root
            
            // Create volume
            Identifier region = regionInjectors[i].getInstance(RegionConfiguration.class).getId();
            ZNodePath path = ZNodePath.root().join(ZNodeLabel.fromString(region.toString()));
            Identifier id = 
                    ControlZNode.CreateEntity.call(ControlSchema.Safari.Volumes.PATH, path, control.materializer()).get();
            
            // Create volume version
            VolumeState state = VolumeState.valueOf(region, ImmutableSet.<Identifier>of());
            Operations.unlessError(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.pathOf(id)).call().get().record());
            Operations.unlessError(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.pathOf(id, version)).call().get().record());
            Operations.unlessError(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Version.State.pathOf(id, version), state).call().get().record());
            Operations.unlessError(control.materializer().create(ControlSchema.Safari.Volumes.Volume.Log.Latest.pathOf(id), version).call().get().record());
            
            volumes.set(
                    i, 
                    Volume.valueOf(
                        id,
                        path,
                        version,
                        region,
                        ImmutableMap.<ZNodeName, Identifier>of()));
        }
        */
        // TODO Update root volume
        
/*
        SingleClientService[] service = new SingleClientService[regionInjectors.length];
        for (int i=0; i<num_regions; ++i) { 
            service[i] = regionInjectors[i].getInstance(SingleClientService.class);
            service[i].startAsync().awaitRunning();
        }*/
        
        // TODO create roots
/*        Message.ServerResponse<?> response = clients[0].getClient().getConnectionClientExecutor().submit(
                Operations.Requests.create().setPath(ZNodePath.root()).build()).get();
        Operations.unlessError(response.record());
        for (int i=0; i<num_regions; ++i) {
            ZNodePath path = (ZNodePath) ZNodePath.joined(BackendSchema.Volumes.path(), rootVolume.getId());
            Identifier assigned = ControlSchema.Volumes.ControlSchema.Region.get(ControlSchema.ControlSchema.Entity.of(rootVolume.getId()), control.materializer()).get().get();
            Identifier region = regionInjectors[i].getInstance(RegionConfiguration.class).getRegion();
            response = backdoors.get(i).submit(
                    Operations.Requests.sync().setPath(path).build()).get();
            Operations.maybeError(response.record(), KeeperException.Code.NONODE);
            response = backdoors.get(i).submit(
                            Operations.Requests.exists().setPath(path).build()).get();
            if (region.equals(assigned)) {
                Operations.unlessError(response.record(), String.valueOf(region));
            } else {
                Operations.expectError(response.record(), KeeperException.Code.NONODE, String.valueOf(region));
            }
        }
        
        // create all volume roots
        for (Volume v: volumes) {
            response = clients[0].getClient().getConnectionClientExecutor().submit(
                    Operations.Requests.create().setPath(v.getSnapshot().getRoot()).build()).get();
            Operations.unlessError(response.record());
            for (int i=0; i<num_regions; ++i) {
                ZNodePath path = (ZNodePath) ZNodePath.joined(BackendSchema.Volumes.path(), v.getId());
                Identifier assigned = ControlSchema.Volumes.ControlSchema.Region.get(ControlSchema.ControlSchema.Entity.of(v.getId()), control.materializer()).get().get();
                Identifier region = regionInjectors[i].getInstance(RegionConfiguration.class).getRegion();
                response = backdoors.get(i).submit(
                        Operations.Requests.sync().setPath(path).build()).get();
                Operations.maybeError(response.record(), KeeperException.Code.NONODE);
                response = backdoors.get(i).submit(
                                Operations.Requests.exists().setPath(path).build()).get();
                if (region.equals(assigned)) {
                    Operations.unlessError(response.record(), String.valueOf(region));
                } else {
                    Operations.expectError(response.record(), KeeperException.Code.NONODE, String.valueOf(region));
                }
            }
        }
*/

        // alternate volume operations for both clients
/*        int iterations = 4;
        int logInterval = 4;
        ListeningExecutorService executor = rootInjector.getInstance(ListeningExecutorService.class);
        Generator<ZNodePath> paths = Generators.iterator(
                Iterators.cycle(
                        Iterables.transform(volumes, 
                            new Function<Volume, ZNodePath>() {
                                @Override
                                public ZNodePath apply(Volume input) {
                                    return input.getDescriptor().getPath();
                                }
                            })));
        List<IteratingClient> clients = Lists.newArrayListWithCapacity(num_regions);
        for (int i=0; i<num_regions; ++i) {
            final CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = 
                    CountingGenerator.create(iterations, logInterval, 
                            SubmitGenerator.create(
                                    PathRequestGenerator.create(paths, Generators.constant(Operations.Requests.exists())), 
                                    service[i].getClient().getConnectionClientExecutor().get().get()), logger);
            IteratingClient client = IteratingClient.create(executor, operations, LoggingPromise.create(logger, SettableFuturePromise.<Void>create()));
            clients.add(client);
            executor.execute(client);
        }
        Futures.successfulAsList(clients).get();
        
        monitor.stopAsync().awaitTerminated(); */
    }
}
