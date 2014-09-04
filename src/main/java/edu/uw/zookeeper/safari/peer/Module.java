package edu.uw.zookeeper.safari.peer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;

import edu.uw.zookeeper.safari.AbstractCompositeSafariModule;

public class Module extends AbstractCompositeSafariModule<Service> {

    public static Class<Peer> annotation() {
        return Peer.class;
    }

    public static Module create() {
        PeerConnectionsService.Module module = PeerConnectionsService.module();
        return new Module(
                module.getKey(),
                ImmutableList.<com.google.inject.Module>of(
                        PeerConfiguration.create(),
                        module));
    }
    
    protected Module(
            Key<? extends Service> key,
            Iterable<? extends com.google.inject.Module> modules) {
        super(key, modules);
    }

    @Override  
    public Key<Service> getKey() {
        return Key.get(Service.class, annotation());
    }
    
    @Provides @Peer
    public Service getService(Injector injector) {
        return getInstance(injector);
    }
}
