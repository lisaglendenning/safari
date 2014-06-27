package edu.uw.zookeeper.safari.control.schema;

import edu.uw.zookeeper.data.RelativeZNodePath;

public final class LinkVolumeLogEntry implements VolumeLogEntry<RelativeZNodePath> {

    public static LinkVolumeLogEntry create(RelativeZNodePath path) {
        return new LinkVolumeLogEntry(path);
    }
    
    protected final RelativeZNodePath path;
    
    public LinkVolumeLogEntry(
            RelativeZNodePath path) {
        this.path = path;
    }
    
    @Override
    public RelativeZNodePath get() {
        return path;
    }
}
