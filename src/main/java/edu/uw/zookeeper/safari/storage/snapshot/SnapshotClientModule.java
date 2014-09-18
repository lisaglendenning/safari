package edu.uw.zookeeper.safari.storage.snapshot;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.client.ConnectionClientExecutor;
import edu.uw.zookeeper.protocol.proto.Records;

public class SnapshotClientModule extends AbstractModule {

    public static SnapshotClientModule create(
            EnsembleView<ServerInetAddressView> fromEnsemble,
            ServerInetAddressView fromAddress,
            ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> fromClient,
            ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> toClient) {
        return new SnapshotClientModule(fromEnsemble, fromAddress, fromClient, toClient);
    }
    
    private final ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> fromClient;
    private final EnsembleView<ServerInetAddressView> fromEnsemble;
    private final ServerInetAddressView fromAddress;
    private final ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> toClient;
    
    protected SnapshotClientModule(
            EnsembleView<ServerInetAddressView> fromEnsemble,
            ServerInetAddressView fromAddress,
            ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> fromClient,
            ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?> toClient) {
        this.fromEnsemble = fromEnsemble;
        this.fromAddress = fromAddress;
        this.fromClient = fromClient;
        this.toClient = toClient;
    }
    
    @Override
    protected void configure() {
        bind(new TypeLiteral<EnsembleView<ServerInetAddressView>>(){}).annotatedWith(From.class).toInstance(fromEnsemble);
        bind(ServerInetAddressView.class).annotatedWith(From.class).toInstance(fromAddress);
        bind(new TypeLiteral<ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?>>(){}).annotatedWith(From.class).toInstance(fromClient);
        bind(new TypeLiteral<ConnectionClientExecutor<? super Records.Request, Message.ServerResponse<?>, SessionListener, ?>>(){}).annotatedWith(To.class).toInstance(toClient);
    }
}
