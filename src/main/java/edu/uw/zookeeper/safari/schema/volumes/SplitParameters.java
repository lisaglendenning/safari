package edu.uw.zookeeper.safari.schema.volumes;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.safari.Identifier;

public class SplitParameters extends AssignParameters {

    public static SplitParameters create(
            ZNodeName branch,
            Identifier leaf,
            Identifier region,
            UnsignedLong version) {
        return new SplitParameters(branch, leaf, region, version);
    }
    
    private final ZNodeName branch;
    private final Identifier leaf;

    @JsonCreator
    public SplitParameters(
            @JsonProperty("branch") ZNodeName branch,
            @JsonProperty("leaf") Identifier leaf,
            @JsonProperty("region") Identifier region,
            @JsonProperty("version") UnsignedLong version) {
        super(region, version);
        this.branch = checkNotNull(branch);
        this.leaf = checkNotNull(leaf);
    }

    public final ZNodeName getBranch() {
        return branch;
    }

    public final Identifier getLeaf() {
        return leaf;
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper toString) {
        return super.toStringHelper(toString).add("branch", branch).add("leaf", leaf);
    }
}
