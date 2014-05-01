package edu.uw.zookeeper.safari.backend;

import javax.annotation.Nullable;

import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.safari.SafariException;

public final class NonResidentVolumeException extends SafariException {

    private static final long serialVersionUID = 7130083610572721152L;
    
    private final @Nullable UnsignedLong version;

    public NonResidentVolumeException(@Nullable UnsignedLong version) {
        this(version, "", null);
    }

    public NonResidentVolumeException(@Nullable UnsignedLong version, String message, Throwable cause) {
        super(message, cause);
        this.version = version;
    }

    public NonResidentVolumeException(@Nullable UnsignedLong version, String message) {
        this(version, message, null);
    }

    public NonResidentVolumeException(@Nullable UnsignedLong version, Throwable cause) {
        this(version, "", cause);
    }
    
    public @Nullable UnsignedLong getVersion() {
        return version;
    }
}
