package edu.uw.zookeeper.orchestra;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;

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
import edu.uw.zookeeper.orchestra.common.DependsOn;
import edu.uw.zookeeper.orchestra.control.ControlMaterializerService;
import edu.uw.zookeeper.orchestra.frontend.FrontendConfiguration;
import edu.uw.zookeeper.orchestra.frontend.FrontendServerService;
import edu.uw.zookeeper.orchestra.peer.EnsembleMemberService;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.proto.IExistsResponse;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class SingleClientTest {

    public static Injector injector() {
        return BootstrapTest.injector();
    }

    @DependsOn({ 
        ControlMaterializerService.class, 
        EnsembleMemberService.class,
        FrontendServerService.class })
    public static class SingleClientService extends BootstrapTest.SimpleMainService {

        public static class Module extends AbstractModule {

            public static Injector injector() {
                return SingleClientTest.injector().createChildInjector(
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
            
            // create root
            client.getClientConnectionExecutor().submit(Operations.Requests.create().build());
        }
    }

    @Test(timeout=10000)
    public void test() throws Exception {
        Injector injector = SingleClientService.Module.injector();
        SingleClientService client = injector.getInstance(SingleClientService.class);
        client.startAsync().awaitRunning();
        ServiceMonitor monitor = injector.getInstance(ServiceMonitor.class);
        monitor.startAsync().awaitRunning();
        
        Callable<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> callable = 
                CallUntilPresent.create(IterationCallable.create(1, 1, 
                    SubmitCallable.create(
                            PathedRequestGenerator.exists(
                                    ConstantGenerator.of(ZNodeLabel.Path.root())), 
                            client.getClient().getClientConnectionExecutor())));
        Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> result = callable.call();
        assertTrue(result.second().get() instanceof IExistsResponse);
        monitor.stopAsync().awaitTerminated();
    }
}
