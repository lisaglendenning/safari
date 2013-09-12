package edu.uw.zookeeper.safari.peer.protocol;

import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.safari.Identifier;

public abstract class ShardedMessage<T extends Encodable> extends EncodableMessage<Identifier, T> {

    protected ShardedMessage(Identifier id, T value) {
        super(id, value);
    }
}
