package edu.uw.zookeeper.safari;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.Callable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.clients.SimpleClientsBuilder;
import edu.uw.zookeeper.clients.common.CallUntilPresent;
import edu.uw.zookeeper.clients.common.IterationCallable;
import edu.uw.zookeeper.clients.common.SubmitCallable;
import edu.uw.zookeeper.clients.random.ConstantGenerator;
import edu.uw.zookeeper.clients.random.PathedRequestGenerator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutor;
import edu.uw.zookeeper.protocol.proto.IExistsResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.frontend.FrontendConfiguration;

@RunWith(JUnit4.class)
public class MultipleClientTest {

    public static Injector injector() {
        return BootstrapTest.injector();
    }

    public static class MultipleClientService extends BootstrapTest.SimpleMainService {

        public static class Module extends AbstractModule {

            public static Injector injector() {
                return MultipleClientTest.injector().createChildInjector(
                        new Module());
            }
            
            @Override
            protected void configure() {
                bind(MultipleClientService.class).in(Singleton.class);
            }
            
            @Provides @Singleton
            public SimpleClientsBuilder getClient(
                    FrontendConfiguration configuration,
                    NetClientModule clientModule,
                    RuntimeModule runtime) {
                SimpleClientsBuilder builder = 
                        SimpleClientsBuilder.defaults(configuration.getAddress(), clientModule)
                        .setRuntimeModule(runtime)
                        .setDefaults();
                return builder;
            }
        }
        
        protected final SimpleClientsBuilder client;
        
        @Inject
        protected MultipleClientService(
                Injector injector,
                SimpleClientsBuilder client) {
            super(injector);
            this.client = client;
        }
        
        public SimpleClientsBuilder getClient() {
            return client;
        }

        @Override
        protected void startUp() throws Exception {
            super.startUp();
            
            for (Service e: client.build()) {
                injector.getInstance(ServiceMonitor.class).addOnStart(e);
                e.startAsync().awaitRunning();
            }

            // create root
            ClientConnectionExecutor<?> connection = client.getClientConnectionExecutors().get().get();
            connection.submit(Operations.Requests.create().build());
            connection.submit(Operations.Requests.disconnect().build()).get();
        }
    }

    @Test(timeout=60000)
    public void testPipeline() throws Exception {
        Injector injector = MultipleClientService.Module.injector();
        MultipleClientService client = injector.getInstance(MultipleClientService.class);
        client.startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        
        int nclients = 2;
        int iterations = 2;
        int logInterval = 10;
        List<ListenableFuture<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>>> futures = Lists.newArrayListWithCapacity(nclients);
        for (int i=0; i<nclients; ++i) {
            Callable<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> callable = 
                    CallUntilPresent.create(IterationCallable.create(iterations, logInterval, 
                        SubmitCallable.create(
                                PathedRequestGenerator.exists(
                                        ConstantGenerator.of(ZNodeLabel.Path.root())), 
                                client.getClient().getClientConnectionExecutors().get().get())));
            futures.add(injector.getInstance(ListeningExecutorService.class).submit(callable));
        }
        List<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> results = Futures.allAsList(futures).get();
        for (Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> result: results) {
            assertTrue(result.second().get().record() instanceof IExistsResponse);
        }
        
        monitor.stopAsync().awaitTerminated();
    }
}
