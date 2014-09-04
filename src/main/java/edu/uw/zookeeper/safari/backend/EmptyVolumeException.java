package edu.uw.zookeeper.safari.backend;

import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.SafariException;

public final class EmptyVolumeException extends SafariException {

    private static final long serialVersionUID = 5935909147249848303L;

    private final Identifier volume;
    
    public EmptyVolumeException(Identifier volume) {
        this(volume, "", null);
    }

    public EmptyVolumeException(Identifier volume, String message, Throwable cause) {
        super(message, cause);
        this.volume = volume;
    }

    public EmptyVolumeException(Identifier volume, String message) {
        this(volume, message, null);
    }

    public EmptyVolumeException(Identifier volume, Throwable cause) {
        this(volume, "", cause);
    }
    
    public Identifier getVolume() {
        return volume;
    }
}
