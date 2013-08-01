package edu.uw.zookeeper.orchestra.backend;

import com.google.common.base.Function;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.orchestra.Volume;
import edu.uw.zookeeper.util.Pair;

// TODO: what about volume changes?
public class VolumeShardedOperationTranslators extends ShardedOperationTranslators {

    public static VolumeShardedOperationTranslators of(Function<Identifier, Volume> lookup) {
        return new VolumeShardedOperationTranslators(lookup);
    }
    
    public static ZNodeLabel.Path rootOf(Identifier id) {
        return ZNodeLabel.Path.of(getPrefix(), ZNodeLabel.Component.of(id.toString()));
    }
    
    public static ZNodeLabel.Path pathOf(Identifier id, ZNodeLabel path) {
        return ZNodeLabel.Path.of(rootOf(id), path);
    }
    
    public static ZNodeLabel.Path getPrefix() {
        return BackendSchema.getInstance().byElement(BackendSchema.Volumes.class).path();
    }
    
    public static class VolumePrefix implements Function<Identifier, Pair<ZNodeLabel.Path, ZNodeLabel.Path>> {
        protected final Function<Identifier, Volume> lookup;
        
        public VolumePrefix(Function<Identifier, Volume> lookup) {
            this.lookup = lookup;
        }
        
        @Override
        public Pair<ZNodeLabel.Path, ZNodeLabel.Path> apply(Identifier id) {
            Volume volume = lookup.apply(id);
            return Pair.create(volume.getDescriptor().getRoot(), rootOf(volume.getId()));
        }
    }

    public VolumeShardedOperationTranslators(Function<Identifier, Volume> lookup) {
        super(new VolumePrefix(lookup));
    }
}
