package edu.uw.zookeeper.safari.control.volumes;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;

import edu.uw.zookeeper.safari.control.schema.VolumeLogEntryPath;
import edu.uw.zookeeper.safari.schema.volumes.VolumeOperation;

public final class VolumeOperationDirective {
    
    public static VolumeOperationDirective create(
            VolumeLogEntryPath entry,
            VolumeOperation<?> operation, 
            boolean commit) {
        return new VolumeOperationDirective(checkNotNull(entry), checkNotNull(operation), commit);
    }

    private final VolumeLogEntryPath entry;
    private final VolumeOperation<?> operation;
    private final boolean commit;
    
    protected VolumeOperationDirective(VolumeLogEntryPath entry,
            VolumeOperation<?> operation, boolean commit) {
        super();
        this.entry = entry;
        this.operation = operation;
        this.commit = commit;
    }
    
    public VolumeLogEntryPath getEntry() {
        return entry;
    }
    
    public VolumeOperation<?> getOperation() {
        return operation;
    }
    
    public boolean isCommit() {
        return commit;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("entry", entry).add("operation", operation).add("commit", commit).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof VolumeOperationDirective)) {
            return false;
        }
        VolumeOperationDirective other = (VolumeOperationDirective) obj;
        return getEntry().equals(other.getEntry()) 
                && getOperation().equals(other.getOperation()) 
                && (isCommit() == other.isCommit());
    }
    
    @Override
    public int hashCode() {
        return getEntry().hashCode();
    }
}
