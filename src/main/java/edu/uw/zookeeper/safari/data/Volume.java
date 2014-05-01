package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.data.EmptyZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

public final class Volume extends VersionedVolume {

    public static Volume valueOf(
            Identifier id, 
            ZNodePath path, 
            UnsignedLong version,
            Identifier region,
            Map<ZNodeName, Identifier> branches) {
        return valueOf(VolumeDescriptor.valueOf(id, path), version, region, branches);
    }

    public static Volume valueOf(
            VolumeDescriptor descriptor, 
            UnsignedLong version,
            Identifier region,
            Map<ZNodeName, Identifier> branches) {
        return new Volume(descriptor, version, region, branches);
    }

    public static boolean contains(final Volume volume, final ZNodePath path) {
        return contains(volume.getDescriptor().getPath(), volume.getBranches().keySet(), path);
    }
    
    public static boolean contains(
            final ZNodePath root, final Set<ZNodeName> branches, final ZNodePath path) {
        if (! path.startsWith(root)) {
            return false;
        }
        ZNodeName remaining = path.suffix(root.isRoot() ? 0 : root.length());
        return contains(branches, remaining);
    }
    
    public static boolean contains(final Set<ZNodeName> branches, ZNodeName remaining) {
        if (! (remaining instanceof EmptyZNodeLabel)) {
            for (ZNodeName branch: branches) {
                if (branch.startsWith(remaining)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public static class Canonicalize implements Function<ZNodeName, ZNodeName> {
        
        public static Canonicalize of(ZNodePath root) {
            return new Canonicalize(root);
        }
        
        private final ZNodePath root;
        
        public Canonicalize(ZNodePath root) {
            this.root = root;
        }

        @Override
        public ZNodeName apply(final ZNodeName input) {
            ZNodeName output = input;
            if (input instanceof ZNodePath) {
                ZNodePath path = (ZNodePath) input;
                checkArgument(path.startsWith(root), input);
                return path.suffix(root.isRoot() ? 0 : root.length());
            }
            return output;
        }
    }

    
    private final Identifier region;
    private final ImmutableBiMap<ZNodeName, Identifier> branches;
    
    public Volume(
            VolumeDescriptor descriptor, 
            UnsignedLong version,
            Identifier region,
            Map<ZNodeName, Identifier> branches) {
        super(descriptor, version);
        this.region = checkNotNull(region);
        this.branches = ImmutableBiMap.copyOf(branches);
    }

    public Identifier getRegion() {
        return region;
    }
    
    public ImmutableBiMap<ZNodeName, Identifier> getBranches() {
        return branches;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("descriptor", getDescriptor())
                .add("version", getVersion())
                .add("region", region)
                .add("branches", branches).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof Volume)) {
            return false;
        }
        Volume other = (Volume) obj;
        return getDescriptor().equals(other.getDescriptor())
                && getDescriptor().equals(other.getDescriptor())
                && region.equals(other.region)
                && branches.equals(other.branches);
    }
}
