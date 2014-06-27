package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.VersionedId;

public final class ShardedErrorResponseMessage extends ShardedResponseMessage<Pair<Integer,SafariException>> {

    public static ShardedErrorResponseMessage valueOf(
            VersionedId shard, int xid,  SafariException exception) {
        return new ShardedErrorResponseMessage(shard, Integer.valueOf(xid), exception);
    }

    @JsonCreator
    public ShardedErrorResponseMessage(
            @JsonProperty("shard") VersionedId shard,
            @JsonProperty("xid") Integer xid,
            @JsonProperty("exception") SafariException exception) {
        super(shard, Pair.create(xid, exception));
    }

    @JsonProperty("xid")
    @Override
    public int xid() {
        return value.first().intValue();
    }

    public SafariException getException() {
        return value.second();
    }
}
