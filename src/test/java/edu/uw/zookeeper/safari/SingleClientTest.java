package edu.uw.zookeeper.safari;

import static org.junit.Assert.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.SimpleClientBuilder;
import edu.uw.zookeeper.clients.SubmitGenerator;
import edu.uw.zookeeper.clients.common.CountingGenerator;
import edu.uw.zookeeper.clients.common.Generator;
import edu.uw.zookeeper.clients.common.Generators;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.frontend.FrontendConfiguration;

@RunWith(JUnit4.class)
public class SingleClientTest {

    public static Injector injector() {
        return BootstrapTest.injector();
    }

    public static Injector injector(Injector parent) {
        return BootstrapTest.injector(parent);
    }

    public static class SingleClientService extends BootstrapTest.SimpleMainService {

        public static class Module extends AbstractModule {

            public static Injector injector() {
                return injector(SingleClientTest.injector());
            }

            public static Injector injector(Injector parent) {
                return parent.createChildInjector(
                        new Module());
            }
            
            @Override
            protected void configure() {
                bind(SingleClientService.class).in(Singleton.class);
            }
            
            @Provides @Singleton
            public SimpleClientBuilder getClient(
                    FrontendConfiguration configuration,
                    NetClientModule clientModule,
                    RuntimeModule runtime) {
                SimpleClientBuilder builder = 
                        SimpleClientBuilder.defaults(configuration.getAddress(), clientModule)
                        .setRuntimeModule(runtime)
                        .setDefaults();
                return builder;
            }
        }
        
        protected final SimpleClientBuilder client;
        
        @Inject
        protected SingleClientService(
                Injector injector,
                SimpleClientBuilder client) {
            super(injector);
            this.client = client;
        }
        
        public SimpleClientBuilder getClient() {
            return client;
        }

        @Override
        protected void startUp() throws Exception {
            super.startUp();
            
            for (Service e: client.build()) {
                injector.getInstance(ServiceMonitor.class).addOnStart(e);
                e.startAsync().awaitRunning();
            }
        }
    }

    protected final Logger logger = LogManager.getLogger();
    
    @Test(timeout=30000)
    public void testPipeline() throws Exception {
        Injector injector = SingleClientService.Module.injector();
        SingleClientService client = injector.getInstance(SingleClientService.class);
        client.startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();

        // create root
        client.getClient().getConnectionClientExecutor().submit(
                Operations.Requests.create().build());
        
        int iterations = 32;
        int logInterval = 8;
        Generator<? extends Records.Request> requests = Generators.constant(Operations.Requests.exists().setPath(ZNodePath.root()).build());
        CountingGenerator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> operations = 
                CountingGenerator.create(iterations, logInterval, 
                        SubmitGenerator.create(requests, client.getClient().getConnectionClientExecutor()), logger);
        while (operations.hasNext()) {
             Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> operation = operations.next();
             assertFalse(operation.second().get().record() instanceof Operation.Error);
        }
        monitor.stopAsync().awaitTerminated();
    }
}
