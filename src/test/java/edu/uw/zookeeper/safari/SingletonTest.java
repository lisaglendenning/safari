package edu.uw.zookeeper.safari;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.IteratingClient;
import edu.uw.zookeeper.client.SubmitGenerator;
import edu.uw.zookeeper.common.CountingGenerator;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Generators;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.frontend.Frontend;

@RunWith(JUnit4.class)
public class SingletonTest extends AbstractMainTest {

    @Test(timeout=20000)
    public void testStartAndStop() throws Exception {
        final long pause = 8000L;
        final List<Component<?>> components = SafariModules.newSingletonSafari();
        pauseWithComponents(components, pause);
    }

    @Test(timeout=30000)
    public void testClientConnect() throws Exception {
        final long pause = 10000L;
        final List<Component<?>> components = newSingletonServerAndClient();
        pauseWithComponents(components, pause);
    }

    @Test(timeout=30000)
    public void testClientsConnect() throws Exception {
        final long pause = 8000L;
        final int nclients = 2;
        final List<Component<?>> components = newSingletonServerAndClients(nclients);
        pauseWithComponents(
                components, 
                pause);
    }

    @Test(timeout=30000)
    public void testClientPipeline() throws Exception {
        final int iterations = 32;
        final int logInterval = 8;
        final TimeValue timeOut = TimeValue.seconds(15L);
        final List<Component<?>> components = newSingletonServerAndClient();
        final Injector injector = stopping(components);
        final Callable<Void> callable = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                final ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> client = injector.getInstance(Key.get(Component.class, Names.named("client"))).injector().getInstance(ConnectionClientExecutorService.Builder.class).getConnectionClientExecutor();
                final Queue<ListenableFuture<Message.ServerResponse<?>>> futures = Queues.newArrayDeque();
                final Generator<? extends Records.Request> requests = Generators.constant(Operations.Requests.exists().setPath(ZNodePath.root()).build());
                final CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = 
                        CountingGenerator.create(iterations, logInterval, 
                                SubmitGenerator.create(requests, client), logger);
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
                injector,
                callable);
    }

    @Test(timeout=40000)
    public void testClientsPipeline() throws Exception {
        final int nclients = 2;
        final int iterations = 32;
        final int logInterval = 8;
        final List<Component<?>> components = newSingletonServerAndClients(nclients);
        final Injector injector = stopping(components);
        final Callable<Void> callable = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    final Generator<? extends Records.Request> requests = Generators.constant(Operations.Requests.exists().setPath(ZNodePath.root()).build());
                    final ImmutableList.Builder<IteratingClient> clients = ImmutableList.builder();
                    for (int i=0; i<nclients; ++i) {
                        final Component<?> client = injector.getInstance(Key.get(Component.class, Names.named(String.format("client-%d", i))));
                        final CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = 
                                CountingGenerator.create(iterations, logInterval, 
                                        SubmitGenerator.create(requests, 
                                                client.injector().getInstance(ConnectionClientExecutorService.Builder.class).getConnectionClientExecutor()), logger);
                        Executor executor = client.injector().getInstance(Executor.class);
                        IteratingClient iterating = 
                                LoggingFutureListener.listen(
                                        logger,
                                        IteratingClient.create(
                                                executor, 
                                                operations, 
                                                SettableFuturePromise.<Void>create()));
                        clients.add(iterating);
                        executor.execute(iterating);
                    }
                    Futures.successfulAsList(clients.build()).get();
                    return null;
                }
            };
        callWithService(
                injector,
                callable);
    }
    
    protected List<Component<?>> newSingletonServerAndClient() {
        final List<Component<?>> safari = SafariModules.newSingletonSafari();
        final Component<?> client = Modules.newClientComponent(
                safari.get(0), 
                EnsembleView.copyOf(safari.get(safari.size()-1).injector().getInstance(Key.get(ServerInetAddressView.class, Frontend.class))),
                Names.named("client"));
        return ImmutableList.<Component<?>>builder().addAll(safari).add(client).build();
    }
    
    protected List<Component<?>> newSingletonServerAndClients(
            final int nclients) {
        final List<Component<?>> safari = SafariModules.newSingletonSafari();
        final EnsembleView<ServerInetAddressView> ensemble = EnsembleView.copyOf(
                safari.get(safari.size()-1).injector().getInstance(Key.get(ServerInetAddressView.class, Frontend.class)));
        final ImmutableList.Builder<Component<?>> clients = ImmutableList.builder();
        for (int i=0; i<nclients; ++i) {
            clients.add(
                    Modules.newClientComponent(
                            safari.get(0), 
                            ensemble,
                Names.named(String.format("client-%d", i))));
        }
        return ImmutableList.<Component<?>>builder().addAll(safari).addAll(clients.build()).build();
    }
}
