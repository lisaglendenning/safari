package edu.uw.zookeeper.safari.schema.volumes;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.safari.Identifier;

public final class VolumeDescriptor {

    public static VolumeDescriptor valueOf(
            Identifier id,
            ZNodePath path) {
        return new VolumeDescriptor(id, path);
    }
    
    public static VolumeDescriptor none() {
        return Holder.NONE.instance;
    }

    private static enum Holder implements Supplier<VolumeDescriptor> {
        NONE(new VolumeDescriptor(Identifier.zero(), ZNodePath.root()));
        
        private final VolumeDescriptor instance;
        
        private Holder(VolumeDescriptor instance) {
            this.instance = instance;
        }

        @Override
        public VolumeDescriptor get() {
            return instance;
        }
    }
    
    private final Identifier id;
    private final ZNodePath path;
    
    @JsonCreator
    public VolumeDescriptor(
            @JsonProperty("id") Identifier id,
            @JsonProperty("path") ZNodePath path) {
        super();
        this.id = checkNotNull(id);
        this.path = checkNotNull(path);
    }

    public Identifier getId() {
        return id;
    }

    public ZNodePath getPath() {
        return path;
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append(id).append('@').append(path).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof VolumeDescriptor)) {
            return false;
        }
        VolumeDescriptor other = (VolumeDescriptor) obj;
        return Objects.equal(id, other.id)
                && Objects.equal(path, other.path);
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
