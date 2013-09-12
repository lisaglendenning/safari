package edu.uw.zookeeper.safari.backend;

import com.google.common.base.Function;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.data.Volume;

// TODO: what about volume changes?
public class VolumeShardedOperationTranslators extends ShardedOperationTranslators {

    public static VolumeShardedOperationTranslators of(Function<Identifier, Volume> lookup) {
        return new VolumeShardedOperationTranslators(lookup);
    }
    
    public static ZNodeLabel.Path rootOf(Identifier id) {
        return (ZNodeLabel.Path) ZNodeLabel.joined(getPrefix(), id);
    }
    
    public static ZNodeLabel.Path pathOf(Identifier id, Object path) {
        return (ZNodeLabel.Path) ZNodeLabel.joined(rootOf(id), path);
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
    
    @Override
    protected OperationPrefixTranslator newTranslator(Identifier id) {
        if (id.equals(Identifier.zero())) {
            return OperationPrefixTranslator.create(
                    RecordPrefixTranslator.<Records.Request>none(), 
                    RecordPrefixTranslator.<Records.Response>none());
        } else {
            return super.newTranslator(id);
        }
    }
}