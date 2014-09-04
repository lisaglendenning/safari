package edu.uw.zookeeper.safari;

import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Key;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.SubmitGenerator;
import edu.uw.zookeeper.common.CountingGenerator;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Generators;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.control.ControlModules;
import edu.uw.zookeeper.safari.frontend.Frontend;
import edu.uw.zookeeper.safari.storage.StorageModules;

@RunWith(JUnit4.class)
public class MultipleRegionTest extends AbstractMainTest {

    @Test(timeout=30000)
    public void testStartAndStop() throws Exception {
        final long pause = 4000L;
        final int num_regions = 2;
        final Component<?> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        Services.startAndWait(control.injector().getInstance(Service.class));
        
        final ImmutableList.Builder<Component<?>> components = ImmutableList.builder();
        components.add(root);
        components.add(control);
        for (int i=0; i<num_regions; ++i) {
            Named name = Names.named(String.format("storage-%d", i+1));
            Component<?> component = StorageModules.newStorageSingletonEnsemble(root, name);
            Services.startAndWait(component.injector().getInstance(Service.class));
            components.add(component);
            name = Names.named(String.format("region-%d", i+1));
            component = SafariModules.newSingletonSafariServer(component, ImmutableList.of(root, control), name);
            components.add(component);
        }
        pauseWithComponents(components.build(), pause);
    }

    @Test(timeout=40000)
    public void testClientConnect() throws Exception {
        final long pause = 4000L;
        final int num_regions = 2;

        final Component<?> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        Services.startAndWait(control.injector().getInstance(Service.class));

        final List<Component<?>> components = Lists.newLinkedList();
        components.add(root);
        components.add(control);
        for (int i=0; i<num_regions; ++i) {
            Named name = Names.named(String.format("storage-%d", i+1));
            Component<?> component = StorageModules.newStorageSingletonEnsemble(root, name);
            Services.startAndWait(component.injector().getInstance(Service.class));
            components.add(component);
            name = Names.named(String.format("region-%d", i+1));
            component = SafariModules.newSingletonSafariServer(component, ImmutableList.of(root, control), name);
            Services.startAndWait(component.injector().getInstance(Service.class));
            components.add(component);
        }
        
        // pause for regions to set up
        pause(pause);
        
        // connect client to last region
        final Component<?> client = Modules.newClientComponent(
                root, 
                EnsembleView.copyOf(components.get(components.size()-1).injector().getInstance(Key.get(ServerInetAddressView.class, Frontend.class))),
                Names.named("client"));
        components.add(client);
        
        pauseWithComponents(components, pause);
    }

    @Test(timeout=40000)
    public void testClientPipeline() throws Exception {
        final int iterations = 32;
        final int logInterval = 8;
        final long pause = 4000L;
        final int num_regions = 2;
        final TimeValue timeOut = TimeValue.seconds(15L);

        final Component<?> root = Modules.newRootComponent();
        final Component<?> control = ControlModules.newControlSingletonEnsemble(root);
        Services.startAndWait(control.injector().getInstance(Service.class));

        final List<Component<?>> components = Lists.newLinkedList();
        components.add(root);
        components.add(control);
        for (int i=0; i<num_regions; ++i) {
            Named name = Names.named(String.format("storage-%d", i+1));
            Component<?> component = StorageModules.newStorageSingletonEnsemble(root, name);
            Services.startAndWait(component.injector().getInstance(Service.class));
            components.add(component);
            name = Names.named(String.format("region-%d", i+1));
            component = SafariModules.newSingletonSafariServer(component, ImmutableList.of(root, control), name);
            Services.startAndWait(component.injector().getInstance(Service.class));
            components.add(component);
        }
        
        // pause for regions to set up
        pause(pause);
        
        // connect client to last region
        final Component<?> client = Modules.newClientComponent(
                root, 
                EnsembleView.copyOf(components.get(components.size()-1).injector().getInstance(Key.get(ServerInetAddressView.class, Frontend.class))),
                Names.named("client"));
        components.add(client);
        
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
                }
                ListenableFuture<Message.ServerResponse<?>> response;
                while ((response = futures.poll()) != null) {
                    assertFalse(response.get(timeOut.value(), timeOut.unit()).record() instanceof Operation.Error);
                }
                return null;
            }
        };
        callWithService(
                monitored(
                        components,
                        Modules.StoppingServiceMonitorProvider.class),
                callable);
    }
}
