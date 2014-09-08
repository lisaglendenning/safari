package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.safari.VersionedId;

@MessageBodyType(MessageType.MESSAGE_TYPE_XOMEGA)
public class MessageXomega extends AbstractPair<VersionedId, Long> implements MessageBody {

    public static MessageXomega valueOf(VersionedId version, Long xomega) {
        return new MessageXomega(version, xomega);
    }

    @JsonCreator
    public MessageXomega(@JsonProperty("version") VersionedId version, @JsonProperty("xomega") Long xomega) {
        super(version, xomega);
    }
    
    public VersionedId getVersion() {
        return first;
    }
    
    public Long getXomega() {
        return second;
    }
}
