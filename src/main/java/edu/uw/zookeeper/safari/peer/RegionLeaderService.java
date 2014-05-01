package edu.uw.zookeeper.safari.peer;

import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.control.ControlMaterializerService;
import edu.uw.zookeeper.safari.control.ControlSchema;
import edu.uw.zookeeper.safari.control.ControlZNode;

public class RegionLeaderService extends AbstractIdleService {

    public static RegionLeaderService newInstance(RegionRoleService role, ControlMaterializerService control) {
        return new RegionLeaderService(role, control);
    }
    
    protected final RegionRoleService role;
    protected final ControlMaterializerService control;
    
    protected RegionLeaderService(RegionRoleService role, ControlMaterializerService control) {
        this.role = role;
        this.control = control;
    }
    
    @Override
    protected void startUp() throws Exception {
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
                if (regions.ceiling(v).equals(role.getRegion())) {
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

    @Override
    protected void shutDown() throws Exception {
        // TODO Auto-generated method stub   
    }
}
