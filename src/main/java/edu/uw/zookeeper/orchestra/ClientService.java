package edu.uw.zookeeper.orchestra;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.DefaultSessionParametersPolicy;
import edu.uw.zookeeper.server.ExpireSessionsTask;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerConnectionListener;
import edu.uw.zookeeper.server.ServerExecutor;
import edu.uw.zookeeper.server.SessionParametersPolicy;

public class ClientService extends AbstractIdleService {
    
    public static ClientService newInstance(
            RuntimeModule runtime,
            ClientConnectionsModule clientModule,
            ServerConnectionsModule serverModule) {
        BackendService backend = BackendService.newInstance(runtime, clientModule);
        FrontendService frontend = FrontendService.newInstance(runtime, serverModule);

        SessionParametersPolicy policy = DefaultSessionParametersPolicy.create(runtime.configuration());
        ExpiringSessionManager sessions = ExpiringSessionManager.newInstance(runtime.publisherFactory().get(), policy);
        ExpireSessionsTask expires = ExpireSessionsTask.newInstance(sessions, runtime.executors().asScheduledExecutorServiceFactory().get(), runtime.configuration());
        AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        ServerExecutor serverExecutor = ServerExecutor.newInstance(runtime.executors().asListeningExecutorServiceFactory().get(), runtime.publisherFactory(), sessions);
        ServerConnectionListener server = ServerConnectionListener.newInstance(frontend.serverConnections(), serverExecutor, serverExecutor, serverExecutor);

        ClientService service = new ClientService(runtime, frontend, backend);
        return service;
    }

    protected final RuntimeModule runtime;
    protected final FrontendService frontend;
    protected final BackendService backend;
    
    protected ClientService(
            RuntimeModule runtime,
            FrontendService frontend, 
            BackendService backend) {
        this.runtime = runtime;
        this.frontend = frontend;
        this.backend = backend;
    }

    public FrontendService frontend() {
        return frontend;
    }
    
    public BackendService backend() {
        return backend;
    }
    
    @Override
    protected void startUp() throws Exception {
        runtime.serviceMonitor().add(backend);
        //runtime.serviceMonitor().add(expires);
        runtime.serviceMonitor().add(frontend);
    }

    @Override
    protected void shutDown() throws Exception {
    }
}
