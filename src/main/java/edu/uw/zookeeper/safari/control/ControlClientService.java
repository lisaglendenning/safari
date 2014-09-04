package edu.uw.zookeeper.safari.control;

import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.EnsembleViewFactory;
import edu.uw.zookeeper.client.ServerViewFactory;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.control.schema.ControlSchema;
import edu.uw.zookeeper.safari.control.schema.ControlZNode;
import edu.uw.zookeeper.safari.schema.SchemaClientService;

public class ControlClientService extends AbstractModule implements SafariModule {

    public static ControlClientService create() {
        return new ControlClientService();
    }
    
    protected ControlClientService() {}
    
    @Override
    public Key<? extends Service> getKey() {
        return Key.get(new TypeLiteral<SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>>>(){});
    }

    @Provides @Control @Singleton
    public ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> newControlClient(
            @Control EnsembleViewFactory<? extends ServerViewFactory<Session, ? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>>> connections,
            ServiceMonitor monitor,
            ScheduledExecutorService scheduler) {
        ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> instance = 
                ConnectionClientExecutorService.newInstance(connections, scheduler);
        monitor.add(instance);
        return instance;
    }

    @Provides @Control @Singleton
    public Materializer<ControlZNode<?>, Message.ServerResponse<?>> getControlClientMaterializer(
            SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> service) {
        return service.materializer();
    }
    
    @Provides @Singleton
    public SchemaClientService<ControlZNode<?>, Message.ServerResponse<?>> newControlClientService(
            final @Control ClientConnectionFactory<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> connections,
            final @Control ConnectionClientExecutorService<Operation.Request, Message.ServerResponse<?>> client,
            final Serializers.ByteCodec<Object> serializer,
            final ServiceMonitor monitor) {
        SchemaClientService<ControlZNode<?>,Message.ServerResponse<?>> instance = SchemaClientService.create(
                Materializer.<ControlZNode<?>, Message.ServerResponse<?>>fromHierarchy(
                        ControlSchema.class,
                        serializer,
                        client),
                ImmutableList.<Service.Listener>of(
                        new Service.Listener() {
                            @Override
                            public void starting() {
                                Services.startAndWait(connections);
                                Services.startAndWait(client);
                            }
                        }));
        monitor.add(instance);
        return instance;
    }

    @Override
    protected void configure() {
        bind(Key.get(new TypeLiteral<SchemaClientService<ControlZNode<?>, ?>>(){})).to(Key.get(new TypeLiteral<SchemaClientService<ControlZNode<?>, Message.ServerResponse<?>>>(){}));
        bind(Key.get(new TypeLiteral<Materializer<ControlZNode<?>, ?>>(){}, Control.class)).to(Key.get(new TypeLiteral<Materializer<ControlZNode<?>, Message.ServerResponse<?>>>(){}, Control.class));
    }
}
