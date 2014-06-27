package edu.uw.zookeeper.safari.control.schema;

import edu.uw.zookeeper.safari.volume.BoundVolumeOperator;

public final class OperatorVolumeLogEntry implements VolumeLogEntry<BoundVolumeOperator<?>> {

    public static OperatorVolumeLogEntry create(BoundVolumeOperator<?> operator) {
        return new OperatorVolumeLogEntry(operator);
    }
    
    protected final BoundVolumeOperator<?> operator;
    
    public OperatorVolumeLogEntry(
            BoundVolumeOperator<?> operator) {
        this.operator = operator;
    }
    
    @Override
    public BoundVolumeOperator<?> get() {
        return operator;
    }
}
