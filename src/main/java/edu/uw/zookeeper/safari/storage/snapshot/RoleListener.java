package edu.uw.zookeeper.safari.storage.snapshot;

import java.util.List;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import edu.uw.zookeeper.common.FutureTransition;
import edu.uw.zookeeper.safari.SafariModule;
import edu.uw.zookeeper.safari.region.InjectingRoleListener;
import edu.uw.zookeeper.safari.region.RegionRoleService;

public final class RoleListener implements Function<RegionRoleService, List<? extends com.google.inject.Module>> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule implements SafariModule {
        
        protected Module() {}
        
        @Provides @Singleton
        public InjectingRoleListener<RoleListener> newRoleListener(
                final Supplier<FutureTransition<RegionRoleService>> role,
                final Injector injector,
                final SnapshotListener service) {
            return RoleListener.listen(injector, role, service);
        }

        @Override
        public Key<?> getKey() {
            return Key.get(InjectingRoleListener.class, Snapshot.class);
        }

        @Override
        protected void configure() {
            bind(Key.get(InjectingRoleListener.class, Snapshot.class)).to(Key.get(new TypeLiteral<InjectingRoleListener<RoleListener>>(){}));
        }
    }

    public static InjectingRoleListener<RoleListener> listen(
            Injector injector,
            Supplier<FutureTransition<RegionRoleService>> role,
            Service service) {
        final RoleListener instance = new RoleListener();
        return InjectingRoleListener.listen(
                injector, 
                instance, 
                role, 
                instance, 
                LogManager.getLogger(instance),
                service);
    }

    protected RoleListener() {}

    @Override
    public List<? extends com.google.inject.Module> apply(final RegionRoleService input) {
        switch (input.getRole().getRole()) {
        case LEADING:
            return ImmutableList.of(
                    InjectingRoleListener.RegionRoleServiceModule.create(input),
                    GarbageCollectSnapshot.module(),
                    CommitSessionSnapshots.module(),
                    AbortClosedSessionSnapshots.module());
        default:
            return ImmutableList.of();
        }
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).toString();
    }
}
