package edu.uw.zookeeper.safari.storage.snapshot;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.EnsembleRole;
import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.region.AbstractRoleListener;
import edu.uw.zookeeper.safari.region.RegionRoleService;
import edu.uw.zookeeper.safari.schema.SchemaClientService;
import edu.uw.zookeeper.safari.storage.schema.StorageZNode;

public final class RoleListener extends AbstractRoleListener<RoleListener> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public RoleListener newRoleListener(
                final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage,
                final Supplier<FutureTransition<RegionRoleService>> role,
                final SnapshotListener service) {
            RoleListener instance = listen(storage, role, service);
            return instance;
        }

        @Override
        public Key<?> getKey() {
            return Key.get(RoleListener.class);
        }

        @Override
        protected void configure() {
        }
    }
    
    public static <O extends Operation.ProtocolResponse<?>>  RoleListener listen(
            final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage,
            final Supplier<FutureTransition<RegionRoleService>> role,
            final Service service) {
        return Services.listen(new RoleListener(storage, role), service);
    }
    
    private final SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage;
    
    protected RoleListener(
            SchemaClientService<StorageZNode<?>,Message.ServerResponse<?>> storage,
            final Supplier<FutureTransition<RegionRoleService>> role) {
        super(role);
        this.storage = storage;
    }

    @Override
    public void onSuccess(RegionRoleService result) {
        super.onSuccess(result);
        if (result.getRole().getRole() == EnsembleRole.LEADING) {
            CleanSnapshot.listen(storage.materializer(), storage.cacheEvents(), result, result.logger());
            ExpireSnapshotSessions.listen(storage.materializer(), storage.cacheEvents(), result);
        }
    }
}
