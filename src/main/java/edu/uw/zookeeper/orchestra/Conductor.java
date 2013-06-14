package edu.uw.zookeeper.orchestra;

import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.WatchEventPublisher;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchPromiseTrie;
import edu.uw.zookeeper.orchestra.control.ControlClientService;
import edu.uw.zookeeper.orchestra.control.Orchestra;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Publisher;

public class Conductor extends AbstractIdleService implements Publisher {

    public static Conductor newInstance(
            RuntimeModule runtime,
            ClientConnectionsModule clientModule,
            ServerConnectionsModule serverModule) {
        ControlClientService controlClient = 
                ControlClientService.newInstance(runtime, clientModule);
        ClientService clientService = ClientService.newInstance(runtime, clientModule, serverModule);
        Conductor conductor = new Conductor(runtime, controlClient, clientService);
        runtime.serviceMonitor().add(controlClient);
        runtime.serviceMonitor().add(conductor);
        runtime.serviceMonitor().add(clientService);
        return conductor;
    }
    
    protected final RuntimeModule runtime;
    protected final Publisher publisher;
    protected final ControlClientService controlClient;
    protected final WatchPromiseTrie watches;
    protected final ClientService clientService;
    protected volatile EnsembleMember member;

    public Conductor(
            RuntimeModule runtime,
            ControlClientService controlClient,
            ClientService clientService) {
        this.runtime = runtime;
        this.controlClient = controlClient;
        this.clientService = clientService;
        this.publisher = runtime.publisherFactory().get();
        this.watches = WatchPromiseTrie.newInstance();
        this.member = null;
    }
    
    public RuntimeModule runtime() {
        return runtime;
    }
    
    public ControlClientService controlClient() {
        return controlClient;
    }
    
    public WatchPromiseTrie watches() {
        return watches;
    }
    
    public EnsembleMember member() {
        return member;
    }
    
    @Override
    protected void startUp() throws Exception {
        clientService.backend().startAndWait();
        BackendView backendView = clientService.backend().view();
        
        this.register(watches);
        WatchEventPublisher.newInstance(this, controlClient());
        
        Materializer materializer = controlClient().materializer();
        Materializer.Operator operator = materializer.operator();

        ServerInetAddressView myAddress = clientService.frontend().address();

        // Create my entity
        Orchestra.Conductors.Entity myEntity = Orchestra.Conductors.Entity.create(myAddress, materializer);
        
        // Register presence
        Orchestra.Conductors.Entity.Presence entityPresence = Orchestra.Conductors.Entity.Presence.of(myEntity);
        Operation.SessionResult result = operator.create(entityPresence.path()).submit().get();
        Operations.unlessError(result.reply().reply(), result.toString());
        
        // Register backend
        Orchestra.Conductors.Entity.Backend entityBackend = Orchestra.Conductors.Entity.Backend.create(backendView, myEntity, materializer);
        if (! backendView.equals(entityBackend.get())) {
            throw new IllegalStateException(entityBackend.get().toString());
        }
        
        // Find my ensemble
        EnsembleView<ServerInetAddressView> myView = backendView.getEnsemble();
        Orchestra.Ensembles.Entity myEnsemble = Orchestra.Ensembles.Entity.create(myView, materializer);
        
        // Start ensemble member
        this.member = new EnsembleMember(this, myEntity.get(), myEnsemble.get());
        runtime().serviceMonitor().add(member);
        member.start().get();
    }

    @Override
    protected void shutDown() throws Exception {
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }

    @Override
    public void post(Object object) {
        publisher.post(object);
    }
}
