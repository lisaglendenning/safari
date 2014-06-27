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

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.client.OperationClientExecutor;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageSchema;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public class StorageClientService extends SchemaClientService<StorageZNode<?>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public static Module create() {
            return new Module();
        }
        
        protected Module() {}

        @Provides @Storage @Singleton
        public Materializer<StorageZNode<?>, Message.ServerResponse<?>> getStorageClientMaterializer(
                StorageClientService service) throws InterruptedException, ExecutionException {
            return service.materializer();
        }
        
        @Provides @Singleton
        public StorageClientService newStorageClientService(
                Serializers.ByteCodec<Object> serializer,
                @Storage ListenableFuture<? extends OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>>> future,
                ServiceMonitor monitor) throws InterruptedException, ExecutionException {
            StorageClientService instance = StorageClientService.forClient(
                    serializer,
                    future.get(), 
                    ImmutableList.<Service.Listener>of());
            monitor.add(instance);
            return instance;
        }

        @Override
        protected void configure() {
            bind(Key.get(new TypeLiteral<Materializer<StorageZNode<?>, ?>>(){}, Storage.class)).to(Key.get(new TypeLiteral<Materializer<StorageZNode<?>, Message.ServerResponse<?>>>(){}, Storage.class));
        }
    }
    
    public static StorageClientService forClient(
            Serializers.ByteCodec<Object> serializer,
            final OperationClientExecutor<? extends ProtocolConnection<? super Message.ClientSession,? extends Operation.Response,?,?,?>> client,
            Iterable<? extends Service.Listener> listeners) {
        return create(
                Materializer.<StorageZNode<?>, Message.ServerResponse<?>>fromHierarchy(
                    StorageSchema.class,
                    serializer,
                    client),
                ImmutableList.<Service.Listener>builder()
                    .addAll(listeners)
                    .add(new Service.Listener() {
                        @Override
                        public void starting() {
                            try {
                                client.session().get();
                            } catch (Exception e) {
                                throw Throwables.propagate(e);
                            }
                        }
                        @Override
                        public void stopping(State from) {
                            try {
                                ConnectionClientExecutorService.disconnect(client);
                            } catch (Exception e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }).build());
    }
    
    public static StorageClientService create(
            Materializer<StorageZNode<?>, Message.ServerResponse<?>> materializer,
            Iterable<? extends Service.Listener> listeners) {
        return new StorageClientService(materializer, listeners);
    }

    protected StorageClientService(
            Materializer<StorageZNode<?>, Message.ServerResponse<?>> materializer,
            Iterable<? extends Service.Listener> listeners) {
        super(materializer, listeners);
    }
}
