package edu.uw.zookeeper.safari.volume;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
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
        this.region = region;
    }

    public final Identifier getRegion() {
        return region;
    }

    @Override
    protected Objects.ToStringHelper toStringHelper(Objects.ToStringHelper toString) {
        return super.toStringHelper(toString).add("region", region);
    }
}
