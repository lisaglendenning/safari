package edu.uw.zookeeper.safari;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;

import edu.uw.zookeeper.DefaultRuntimeModule;
import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.clients.common.Generators;
import edu.uw.zookeeper.clients.common.IterationCallable;
import edu.uw.zookeeper.clients.common.SubmitCallable;
import edu.uw.zookeeper.clients.random.PathedRequestGenerator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.SingleClientTest.SingleClientService;
import edu.uw.zookeeper.safari.backend.BackendSchema;
import edu.uw.zookeeper.safari.backend.SimpleBackendConnections;
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
        Volume[] volumes = new Volume[num_regions];
        for (int i=0; i<num_regions; ++i) { 
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
        for (int i=0; i<num_regions; ++i) { 
            clients[i] = regionInjectors[i].getInstance(SingleClientService.class);
            clients[i].startAsync().awaitRunning();
        }
        
        // create root
        Message.ServerResponse<?> response = clients[0].getClient().getConnectionClientExecutor().submit(
                Operations.Requests.create().setPath(rootVolume.getDescriptor().getRoot()).build()).get();
        Operations.unlessError(response.record());
        for (int i=0; i<num_regions; ++i) {
            ZNodeLabel.Path path = (ZNodeLabel.Path) ZNodeLabel.joined(BackendSchema.Volumes.path(), rootVolume.getId());
            Identifier assigned = ControlSchema.Volumes.Entity.Region.get(ControlSchema.Volumes.Entity.of(rootVolume.getId()), control.materializer()).get().get();
            Identifier region = regionInjectors[i].getInstance(EnsembleConfiguration.class).getEnsemble();
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
                    Operations.Requests.create().setPath(v.getDescriptor().getRoot()).build()).get();
            Operations.unlessError(response.record());
            for (int i=0; i<num_regions; ++i) {
                ZNodeLabel.Path path = (ZNodeLabel.Path) ZNodeLabel.joined(BackendSchema.Volumes.path(), v.getId());
                Identifier assigned = ControlSchema.Volumes.Entity.Region.get(ControlSchema.Volumes.Entity.of(v.getId()), control.materializer()).get().get();
                Identifier region = regionInjectors[i].getInstance(EnsembleConfiguration.class).getEnsemble();
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

        // alternate volume operations for both clients
        int iterations = 4;
        int logInterval = 4;
        List<Callable<Optional<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>>>> callables = Lists.newArrayListWithCapacity(num_regions);
        for (int i=0; i<num_regions; ++i) {
            callables.add(
                IterationCallable.create(iterations, logInterval,
                    SubmitCallable.create(
                        PathedRequestGenerator.exists(
                                Generators.cycle(
                                        Iterables.transform(
                                            Arrays.asList(volumes),
                                            new Function<Volume, ZNodeLabel.Path>() {
                                                @Override
                                                public ZNodeLabel.Path apply(Volume input) {
                                                    return input.getDescriptor().getRoot();
                                                }}))), 
                                clients[i].getClient().getConnectionClientExecutor().get().get())));
        }
        List<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> results =
            CallUntilAllPresent.create(
                    ListCallable.create(callables)).call();
        for (Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> result: results) {
            Operations.unlessError(result.second().get().record());
        }
        
        for (OperationClientExecutor<?> backdoor: backdoors) {
            Operations.unlessError(ConnectionClientExecutorService.disconnect(backdoor).record());
        }
        
        monitor.stopAsync().awaitTerminated();
    }
    
    public static class CallUntilAllPresent<V> implements Callable<List<V>> {

        public static <V> CallUntilAllPresent<V> create(
                Callable<List<Optional<V>>> callable) {
            return new CallUntilAllPresent<V>(callable);
        }
        
        private final Callable<List<Optional<V>>> callable;
        
        public CallUntilAllPresent(
                Callable<List<Optional<V>>> callable) {
            this.callable = callable;
        }
        
        @Override
        public List<V> call() throws Exception {
            List<V> results = null;
            while (results == null) {
                boolean absent = false;
                List<Optional<V>> partial = callable.call();
                for (Optional<V> e: partial) {
                    if (! e.isPresent()) {
                        absent = true;
                        break;
                    }
                }
                if (! absent) {
                    results = Lists.newArrayListWithCapacity(partial.size());
                    for (Optional<V> e: partial) {
                        results.add(e.get());
                    }
                }
            }
            return results;
        }
    }
    
    public static class ListCallable<V> implements Callable<List<V>> {

        public static <V> ListCallable<V> create (List<Callable<V>> callables) {
            return new ListCallable<V>(callables);
        }
        
        private final List<Callable<V>> callables;
        
        public ListCallable(List<Callable<V>> callables) {
            this.callables = callables;
        }
        
        @Override
        public List<V> call() throws Exception {
            List<V> results = Lists.newArrayListWithCapacity(callables.size());
            for (Callable<V> callable: callables) {
                results.add(callable.call());
            }
            return results;
        }
        
    }
}
