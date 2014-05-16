package edu.uw.zookeeper.safari.region;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import edu.uw.zookeeper.common.ServiceListenersService;
import edu.uw.zookeeper.common.Services;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlClientService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;
import edu.uw.zookeeper.safari.data.VolumeCacheService;

public class RegionLeaderService extends ServiceListenersService {

    public static Module module() {
        return new Module();
    }
    
    public static class Module extends AbstractModule {

        public Module() {}
        
        @Provides
        public RegionLeaderService getRegionLeaderService(
                @Region Identifier id,
                ControlClientService control,
                VolumeCacheService volumes) {
            RegionLeaderService instance = RegionLeaderService.create(
                    id, control, 
                    volumes);
            return instance;
        }

        @Override
        protected void configure() {
        }
    }
    
    public static RegionLeaderService create(
            Identifier region, 
            ControlClientService control, 
            final VolumeCacheService volumes) {
        return new RegionLeaderService(region, control, ImmutableList.of(
                new Service.Listener() {
                    @Override
                    public void starting() {
                        Services.startAndWait(volumes);
                    }
                }));
    }
    
    protected final Identifier region;
    protected final ControlClientService control;
    
    protected RegionLeaderService(
            Identifier region, ControlClientService control,
            Iterable<? extends Service.Listener> listeners) {
        super(listeners);
        this.region = region;
        this.control = control;
    }
    
    @Override
    protected void startUp() throws Exception {
        logger.info("STARTING {}", this);
        super.startUp();
        
        // create root volume if there are no volumes
        control.materializer().sync(ControlSchema.Safari.Volumes.PATH).call();
        Operation.ProtocolResponse<?> response = control.materializer().getChildren(ControlSchema.Safari.Volumes.PATH).call().get();
        if (((Records.ChildrenGetter) response.record()).getChildren().isEmpty()) {
            ControlZNode.CreateEntity.call(
                    ControlSchema.Safari.Volumes.PATH, ZNodePath.root(), control.materializer()).get();
        }
        
        // Calculate "my" volumes using distance in the identifier space
        Identifier.Space regions = Identifier.Space.newInstance();
        List<Identifier> myVolumes = Lists.newLinkedList();
        control.materializer().cache().lock().readLock().lock();
        try {
            for (ZNodeName name: control.materializer().cache().cache().get(ControlSchema.Safari.Regions.PATH).keySet()) {
                regions.add(Identifier.valueOf(name.toString()));
            }
            for (Map.Entry<ZNodeName, ControlZNode<?>> e: control.materializer().cache().cache().get(ControlSchema.Safari.Volumes.PATH).entrySet()) {
                Identifier v = Identifier.valueOf(e.getKey().toString());
                if (regions.ceiling(v).equals(region)) {
                    myVolumes.add(v);
                }
            }
        } finally {
            control.materializer().cache().lock().readLock().unlock();
        }
        
        // Try to acquire my volumes if open for grabs
        for (Identifier v: myVolumes) {
            // FIXME
//            ControlSchema.Volumes.Entity.Region.create(role.getRegion(), v, control.materializer());
        }
    }
}
