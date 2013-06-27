package edu.uw.zookeeper.orchestra.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SessionHeader {
    
    private final long sessionId;
    
    @JsonCreator
    public SessionHeader(@JsonProperty("sessionId") long sessionId) {
        this.sessionId = sessionId;
    }
    
    public long getSessionId() {
        return sessionId;
    }
}
