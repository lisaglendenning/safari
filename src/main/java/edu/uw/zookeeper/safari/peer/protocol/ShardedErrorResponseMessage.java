package edu.uw.zookeeper.safari.peer.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import edu.uw.zookeeper.safari.SafariException;
import edu.uw.zookeeper.safari.VersionedId;

public final class ShardedErrorResponseMessage extends ShardedResponseMessage<ShardedErrorResponseMessage.ErrorResponse> {

    public static ShardedErrorResponseMessage valueOf(
            VersionedId shard, int xid,  SafariException exception) {
        return valueOf(shard, new ErrorResponse(xid, exception));
    }

    public static ShardedErrorResponseMessage valueOf(
            VersionedId shard, ErrorResponse error) {
        return new ShardedErrorResponseMessage(shard, error);
    }
    
    @JsonCreator
    public ShardedErrorResponseMessage(
            @JsonProperty("shard") VersionedId shard,
            @JsonProperty("error") ErrorResponse error) {
        super(shard, error);
    }

    @JsonProperty("xid")
    @Override
    public int xid() {
        return value.getXid();
    }

    public SafariException getError() {
        return value.getException();
    }
    
    public static final class ErrorResponse {
        
        private final int xid;
        private final SafariException exception;

        @JsonCreator
        public ErrorResponse(
                @JsonProperty("xid") int xid, 
                @JsonProperty("exception") SafariException exception) {
            super();
            this.xid = xid;
            this.exception = exception;
        }

        public int getXid() {
            return xid;
        }

        public SafariException getException() {
            return exception;
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("xid", getXid()).add("exception", getException()).toString();
        }
    }
}
