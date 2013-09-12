package edu.uw.zookeeper.safari.frontend;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedLookup;
import edu.uw.zookeeper.safari.common.CachedLookupService;
import edu.uw.zookeeper.safari.common.SharedLookup;
import edu.uw.zookeeper.safari.control.Control;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;

public class PeerToEnsembleLookup extends CachedLookupService<Identifier, Identifier> {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Override
        protected void configure() {
        }

        @Provides @Singleton
        public PeerToEnsembleLookup getPeerToEnsembleLookup(
                ControlMaterializerService control) {
            return PeerToEnsembleLookup.newInstance(control.materializer());
        }
    }
    
    public static PeerToEnsembleLookup newInstance(
            Materializer<?> materializer) {
        ConcurrentMap<Identifier, Identifier> cache = new MapMaker().makeMap();
        CachedLookup<Identifier, Identifier> lookup = 
                CachedLookup.create(
                        cache,
                        SharedLookup.create(
                                ControlSchema.Peers.Entity.lookupEnsemble(materializer)));
        return newInstance(materializer, lookup);
    }
    
    public static PeerToEnsembleLookup newInstance(
            Materializer<?> materializer,
            CachedLookup<Identifier, Identifier> cache) {
        return new PeerToEnsembleLookup(materializer, cache);
    }
    
    protected static final ZNodeLabel.Path ENSEMBLES_PATH = Control.path(ControlSchema.Ensembles.class);

    public PeerToEnsembleLookup(
            Materializer<?> materializer,
            CachedLookup<Identifier, Identifier> cache) {
        super(materializer, cache);
    }

    @Subscribe
    public void handleNodeUpdate(ZNodeViewCache.NodeUpdate event) {
        ZNodeLabel.Path path = event.path().get();
        if (! ENSEMBLES_PATH.prefixOf(path)) {
            return;
        }
    
        ImmutableList<ZNodeLabel.Component> components = ImmutableList.copyOf(path);
        assert (components.size() >= 2);
        if (components.size() == 2) {
            if (event.type() == ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED) {
                cache.asCache().clear();
            }
        } else {
            Identifier ensembleId = Identifier.valueOf(components.get(2).toString());
            if (components.size() == 3) {
                if (event.type() == ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED) {
                    for (Map.Entry<Identifier, Identifier> e: cache.asCache().entrySet()) {
                        if (e.getValue().equals(ensembleId)) {
                            cache.asCache().remove(e.getKey(), e.getValue());
                        }
                    }
                }
            } else {
                if (components.get(3).equals(ControlSchema.Ensembles.Entity.Peers.LABEL)) {
                    if (components.size() == 4) {
                        if (event.type() == ZNodeViewCache.NodeUpdate.UpdateType.NODE_REMOVED) {
                            for (Map.Entry<Identifier, Identifier> e: cache.asCache().entrySet()) {
                                if (e.getValue().equals(ensembleId)) {
                                    cache.asCache().remove(e.getKey(), e.getValue());
                                }
                            }
                        }
                    } else {
                        Identifier peerId = Identifier.valueOf(components.get(4).toString());
                        switch (event.type()) {
                        case NODE_REMOVED:
                            cache.asCache().remove(peerId, ensembleId);
                            break;
                        case NODE_ADDED:
                            cache.asCache().put(peerId, ensembleId);
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void startUp() throws Exception {
        super.startUp();
        
        Materializer.MaterializedNode ensembles = materializer.get(ENSEMBLES_PATH);
        if (ensembles != null) {
            for (Map.Entry<ZNodeLabel.Component, Materializer.MaterializedNode> ensemble: ensembles.entrySet()) {
                Identifier ensembleId = Identifier.valueOf(ensemble.getKey().toString());
                Materializer.MaterializedNode peers = ensemble.getValue().get(ControlSchema.Ensembles.Entity.Peers.LABEL);
                if (peers != null) {
                    for (ZNodeLabel.Component peer: peers.keySet()) {
                        Identifier peerId = Identifier.valueOf(peer.toString());
                        cache.asCache().put(peerId, ensembleId);
                    }
                }
            }
        }
    }
}