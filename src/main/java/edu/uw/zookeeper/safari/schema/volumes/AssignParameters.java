package edu.uw.zookeeper.safari.schema.volumes;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.UnsignedLong;

import edu.uw.zookeeper.safari.Identifier;

public class AssignParameters extends VolumeOperatorParameters {

    public static AssignParameters create(
            Identifier region,
            UnsignedLong version) {
        return new AssignParameters(region, version);
    }
    
    private final Identifier region;

    @JsonCreator
    public AssignParameters(
            @JsonProperty("region") Identifier region,
            @JsonProperty("version") UnsignedLong version) {
        super(version);
        this.region = checkNotNull(region);
    }

    public final Identifier getRegion() {
        return region;
    }

    @Override
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper toString) {
        return super.toStringHelper(toString).add("region", region);
    }
}
