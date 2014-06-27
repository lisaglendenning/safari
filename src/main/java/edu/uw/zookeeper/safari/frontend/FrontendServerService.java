package edu.uw.zookeeper.safari.frontend;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.client.CreateOrEquals;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.ConnectionFactory.ConnectionsListener;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;
import edu.uw.zookeeper.protocol.server.ServerExecutor;
import edu.uw.zookeeper.protocol.server.ServerProtocolConnection;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.peer.Peer;

public class FrontendServerService extends ServiceListenersService {

    public static com.google.inject.Module module() {
        return Module.create();
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        protected Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Frontend @Singleton
        public ServerConnectionsHandler<? extends ServerProtocolConnection<?,?>> newServerConnectionsHandler(
                @Frontend TimeValue timeOut,
                @Frontend ServerExecutor<FrontendSessionExecutor> server,
                FrontendServerExecutor serverService,
                ScheduledExecutorService scheduler,
                ServiceMonitor monitor) {
            ServerConnectionsHandler<? extends ServerProtocolConnection<?,?>> handler = 
                    ServerConnectionsHandler.create(server, scheduler, timeOut);
            monitor.add(handler);
            return handler;
        }
        
        @Provides @Singleton
        public FrontendServerService getFrontendServerService(
                final @Frontend ServerConnectionsHandler<? extends ServerProtocolConnection<?,?>> handler,
                final @Frontend ServerConnectionFactory<? extends ServerProtocolConnection<?,?>> connections,
                @Peer Identifier peer,
                @Frontend ServerInetAddressView address,
                ControlClientService control,
                ServiceMonitor monitor) throws Exception {
            handler.addListener(
                    new Service.Listener() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public void running() {
                            connections.subscribe((ConnectionsListener<? super ServerProtocolConnection<?,?>>) handler);
                        }
                        
                        @SuppressWarnings("unchecked")
                        @Override
                        public void stopping(State from) {
                            connections.unsubscribe((ConnectionsListener<? super ServerProtocolConnection<?,?>>) handler);
                        }
                    }, SameThreadExecutor.getInstance());
            FrontendServerService instance = FrontendServerService.create(
                    peer,
                    address,
                    control.materializer(),
                    ImmutableList.<Service.Listener>of());
            monitor.add(instance);
            return instance;
        }
    }

    public static FrontendServerService create(
            Identifier peer,
            ServerInetAddressView address,
            Materializer<ControlZNode<?>,?> control,
            Iterable<? extends Service.Listener> listeners) {
        FrontendServerService instance = new FrontendServerService(
                ImmutableList.<Service.Listener>builder()
                .addAll(listeners)
                .add(new Advertiser(peer, address, control))
                .build());
        return instance;
    }
    
    public static ListenableFuture<Optional<ServerInetAddressView>> advertise(
            final Identifier peer, 
            final ServerInetAddressView value, 
            final Materializer<ControlZNode<?>,?> materializer) {    
        ZNodePath path = ControlSchema.Safari.Peers.Peer.ClientAddress.pathOf(peer);
        ListenableFuture<Optional<ServerInetAddressView>> future = CreateOrEquals.create(path, value, materializer, SettableFuturePromise.<Optional<ServerInetAddressView>>create());
        return Futures.transform(future, 
                new Function<Optional<ServerInetAddressView>, Optional<ServerInetAddressView>>() {
                    @Override
                    public Optional<ServerInetAddressView> apply(
                            Optional<ServerInetAddressView> input) {
                        if (input.isPresent()) {
                            throw new IllegalStateException(String.format("%s != %s", value, input.get()));
                        }
                        return input;
                    }
        });
    }
    
    protected FrontendServerService(
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
    }

    @Override
    protected Executor executor() {
        return SameThreadExecutor.getInstance();
    }

    protected static class Advertiser extends Service.Listener {

        protected final Identifier peer;
        protected final ServerInetAddressView value;
        protected final Materializer<ControlZNode<?>,?> control;
        
        public Advertiser(Identifier peer, ServerInetAddressView value, Materializer<ControlZNode<?>,?> control) {
            this.peer = peer;
            this.value = value;
            this.control = control;
        }
        
        @Override
        public void running() {
            try {
                advertise(peer, value, control).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
