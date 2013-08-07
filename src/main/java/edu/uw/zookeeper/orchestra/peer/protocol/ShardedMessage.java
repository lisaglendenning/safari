package edu.uw.zookeeper.orchestra.peer.protocol;

import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.protocol.Encodable;

public abstract class ShardedMessage<T extends Encodable> extends EncodableMessage<Identifier, T> {

    protected ShardedMessage(Identifier id, T value) {
        super(id, value);
    }
}
