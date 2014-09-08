package edu.uw.zookeeper.safari.storage;

import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.client.AbstractConnectionClientExecutor;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class StorageClientService extends AbstractModule implements SafariModule {

    public static StorageClientService create() {
        return new StorageClientService();
    }
    
    protected StorageClientService() {}

    @Override
    public Key<? extends Service> getKey() {
        return Key.get(new TypeLiteral<SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>>>(){});
    }

    @Provides @Singleton
    public SchemaClientService<StorageZNode<?>, Message.ServerResponse<?>> newStorageClientService(
            final Serializers.ByteCodec<Object> serializer,
            final @Storage ListenableFuture<? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> future,
            final ServiceMonitor monitor) throws InterruptedException, ExecutionException {
        final OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> client = future.get();
        SchemaClientService<StorageZNode<?>, Message.ServerResponse<?>> instance = SchemaClientService.create(
                Materializer.<StorageZNode<?>, Message.ServerResponse<?>>fromHierarchy(
                        StorageSchema.class,
                        serializer,
                        client),
                ImmutableList.<Service.Listener>of(
                        new Service.Listener() {
                            @Override
                            public void starting() {
                                try {
                                    client.session().get();
                                } catch (Exception e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                            @Override
                            public void stopping(Service.State from) {
                                try {
                                    AbstractConnectionClientExecutor.disconnect(client);
                                } catch (Exception e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        }));
        monitor.add(instance);
        return instance;
    }

    @Provides @Storage @Singleton
    public Materializer<StorageZNode<?>, Message.ServerResponse<?>> getStorageClientMaterializer(
            SchemaClientService<StorageZNode<?>, Message.ServerResponse<?>> service) {
        return service.materializer();
    }
    
    @Override
    protected void configure() {
        bind(Key.get(new TypeLiteral<SchemaClientService<StorageZNode<?>, ?>>(){})).to(Key.get(new TypeLiteral<SchemaClientService<StorageZNode<?>, Message.ServerResponse<?>>>(){}));
        bind(Key.get(new TypeLiteral<Materializer<StorageZNode<?>, ?>>(){}, Storage.class)).to(Key.get(new TypeLiteral<Materializer<StorageZNode<?>, Message.ServerResponse<?>>>(){}, Storage.class));
    }
}
