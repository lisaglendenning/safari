package edu.uw.zookeeper.safari.backend;

import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.data.VersionedId;

public final class OutdatedVersionException extends SafariException {

    private static final long serialVersionUID = 4025770437962115647L;

    private final VersionedId version;
    
    public OutdatedVersionException(VersionedId version) {
        this(version, "", null);
    }

    public OutdatedVersionException(VersionedId version, String message, Throwable cause) {
        super(message, cause);
        this.version = version;
    }

    public OutdatedVersionException(VersionedId version, String message) {
        this(version, message, null);
    }

    public OutdatedVersionException(VersionedId version, Throwable cause) {
        this(version, "", cause);
    }
    
    public VersionedId getVersion() {
        return version;
    }
}
