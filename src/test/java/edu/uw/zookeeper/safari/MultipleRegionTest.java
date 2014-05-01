package edu.uw.zookeeper.safari;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Guice;
import com.google.inject.Injector;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.clients.IteratingClient;
import edu.uw.zookeeper.clients.SubmitGenerator;
import edu.uw.zookeeper.clients.common.CountingGenerator;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.Generators;
import edu.uw.zookeeper.clients.random.PathRequestGenerator;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SingleClientTest.SingleClientService;
import edu.uw.zookeeper.safari.backend.SimpleBackendConnections;
import edu.uw.zookeeper.safari.common.GuiceRuntimeModule;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlTest;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.control.SimpleControlConnectionsService;
import edu.uw.zookeeper.safari.data.Volume;
import edu.uw.zookeeper.safari.data.VolumeDescriptor;
import edu.uw.zookeeper.safari.data.VolumeState;
import edu.uw.zookeeper.safari.net.IntraVmAsNetModule;
import edu.uw.zookeeper.safari.peer.RegionConfiguration;
import edu.uw.zookeeper.safari.peer.protocol.JacksonModule;

@RunWith(JUnit4.class)
public class MultipleRegionTest {

    public static Injector injector() {
        return Guice.createInjector(
                GuiceRuntimeModule.create(DefaultRuntimeModule.defaults()),
                IntraVmAsNetModule.create(),
                JacksonModule.create(),
                SimpleControlConnectionsService.module());
    }
    
    public static Injector injector(Injector parent) {
        return SingleClientTest.SingleClientService.Module.injector(
                BootstrapTest.SimpleMainService.Module.injector(
                BootstrapTest.injector(
                        parent.createChildInjector(ControlTest.module()))));
    }

    protected final Logger logger = LogManager.getLogger(getClass());
    
    @Test(timeout=30000)
    public void test() throws Exception {
        Injector rootInjector = injector();
        int num_regions = 2;
        
        Injector controlInjector = rootInjector.createChildInjector(ControlTest.module());
        ControlMaterializerService control = controlInjector.getInstance(ControlMaterializerService.class);
        control.startAsync().awaitRunning();
        
        // Create root volume
        VolumeDescriptor rootVd = VolumeDescriptor.valueOf(
                ControlZNode.CreateEntity.call(ControlSchema.Safari.Volumes.PATH, ZNodePath.root(), control.materializer()).get(),
                ZNodePath.root());
        // TODO reserve
        
        Injector[] regionInjectors = new Injector[num_regions];
        for (int i=0; i<num_regions; ++i) { 
            regionInjectors[i] = injector(rootInjector);
        }
        for (Injector injector: regionInjectors) {
            injector.getInstance(BootstrapTest.SimpleMainService.class).startAsync();
        }
        for (Injector injector: regionInjectors) {
            injector.getInstance(BootstrapTest.SimpleMainService.class).awaitRunning();
        }
        
        // open backdoor connections
        List<OperationClientExecutor<?>> backdoors = Lists.newArrayListWithCapacity(num_regions);
        for (Injector injector: regionInjectors) {
            backdoors.add(OperationClientExecutor.newInstance(
                    ConnectMessage.Request.NewRequest.newInstance(), 
                    injector.getInstance(SimpleBackendConnections.class).get().get(),
                    injector.getInstance(ScheduledExecutorService.class)));
        }
        for (OperationClientExecutor<?> backdoor: backdoors) {
            assertTrue(backdoor.session().get() instanceof ConnectMessage.Response.Valid);
        }
        
        ServiceMonitor monitor = rootInjector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();

        // Create and assign a volume for each region
        UnsignedLong version = UnsignedLong.valueOf(1L);
        List<Volume> volumes = Lists.newArrayListWithCapacity(num_regions);
        for (int i=0; i<num_regions; ++i) { 

            // TODO create volume root
            
            // Create volume
            Identifier region = regionInjectors[i].getInstance(RegionConfiguration.class).getRegion();
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
        
        // TODO Update root volume
        

        SingleClientService[] service = new SingleClientService[regionInjectors.length];
        for (int i=0; i<num_regions; ++i) { 
            service[i] = regionInjectors[i].getInstance(SingleClientService.class);
            service[i].startAsync().awaitRunning();
        }
        
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
        int iterations = 4;
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
        
        monitor.stopAsync().awaitTerminated();
    }
}
