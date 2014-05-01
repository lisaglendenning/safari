package edu.uw.zookeeper.safari;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import edu.uw.zookeeper.clients.IteratingClient;
import edu.uw.zookeeper.clients.SimpleClientsBuilder;
import edu.uw.zookeeper.clients.SubmitGenerator;
import edu.uw.zookeeper.clients.common.CountingGenerator;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.Generators;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
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
            ConnectionClientExecutor<Operation.Request,?,?,?> connection = client.getConnectionClientExecutors().get().get();
            connection.submit(Operations.Requests.create().build());
            connection.submit(Operations.Requests.disconnect().build()).get();
        }
    }

    protected final Logger logger = LogManager.getLogger();
    
    @Test(timeout=30000)
    public void testPipeline() throws Exception {
        Injector injector = MultipleClientService.Module.injector();
        MultipleClientService service = injector.getInstance(MultipleClientService.class);
        service.startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        
        int nclients = 2;
        int iterations = 32;
        int logInterval = 8;
        ListeningExecutorService executor = injector.getInstance(ListeningExecutorService.class);
        Generator<? extends Records.Request> requests = Generators.constant(Operations.Requests.exists().setPath(ZNodePath.root()).build());
        List<IteratingClient> clients = Lists.newArrayListWithCapacity(nclients);
        for (int i=0; i<nclients; ++i) {
            final CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = 
                    CountingGenerator.create(iterations, logInterval, 
                            SubmitGenerator.create(requests, service.getClient().getConnectionClientExecutors().get().get()), logger);
            IteratingClient client = IteratingClient.create(executor, operations, LoggingPromise.create(logger, SettableFuturePromise.<Void>create()));
            clients.add(client);
            executor.execute(client);
        }
        Futures.successfulAsList(clients).get();
        
        monitor.stopAsync().awaitTerminated();
    }
}
