package edu.uw.zookeeper.safari.schema.volumes;

import edu.uw.zookeeper.common.AbstractPair;

public class BoundVolumeOperator<T extends VolumeOperatorParameters> extends AbstractPair<VolumeOperator, T> {
    
    public static <T extends VolumeOperatorParameters> BoundVolumeOperator<T> valueOf(VolumeOperator operator, T parameters) {
        return new BoundVolumeOperator<T>(operator, parameters);
    }
    
    protected BoundVolumeOperator(VolumeOperator operator, T parameters) {
        super(operator, parameters);
    }

    public VolumeOperator getOperator() {
        return first;
    }
    
    public T getParameters() {
        return second;
    }
}
