package edu.uw.zookeeper.safari.schema.volumes;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.safari.VersionedId;

public class VolumeOperation<T extends VolumeOperatorParameters> extends AbstractPair<VersionedId, BoundVolumeOperator<T>> {
    
    public static <T extends VolumeOperatorParameters> VolumeOperation<T> valueOf(VersionedId volume, BoundVolumeOperator<T> operator) {
        return new VolumeOperation<T>(volume, operator);
    }
    
    protected VolumeOperation(VersionedId volume, BoundVolumeOperator<T> operator) {
        super(volume, operator);
    }

    public VersionedId getVolume() {
        return first;
    }
    
    public BoundVolumeOperator<T> getOperator() {
        return second;
    }
}
