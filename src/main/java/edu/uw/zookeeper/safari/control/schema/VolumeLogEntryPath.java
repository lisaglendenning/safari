package edu.uw.zookeeper.safari.control.schema;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.data.Sequential;
import edu.uw.zookeeper.safari.VersionedId;

public class VolumeLogEntryPath extends AbstractPair<VersionedId,Sequential<String,?>> {
    
    public static VolumeLogEntryPath valueOf(VersionedId volume, Sequential<String,?> entry) {
        return new VolumeLogEntryPath(volume, entry);
    }
    
    protected VolumeLogEntryPath(VersionedId volume, Sequential<String,?> entry) {
        super(volume, entry);
    }

    public VersionedId volume() {
        return first;
    }
    
    public Sequential<String,?> entry() {
        return second;
    }
    
    public AbsoluteZNodePath path() {
        return ControlSchema.Safari.Volumes.Volume.Log.Version.Entry.pathOf(volume().getValue(), volume().getVersion(), entry());
    }
    
    @Override
    public int hashCode() {
        return path().hashCode();
    }
    
    @Override
    public String toString() {
        return path().toString();
    }
}
