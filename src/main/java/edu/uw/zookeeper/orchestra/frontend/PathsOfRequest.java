package edu.uw.zookeeper.orchestra.frontend;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public enum PathsOfRequest implements Function<Records.Request, List<ZNodeLabel.Path>> {
    INSTANCE;

    public static PathsOfRequest getInstance() {
        return INSTANCE;
    }
    
    @Override
    public List<ZNodeLabel.Path> apply(Records.Request input) {
        ImmutableList.Builder<ZNodeLabel.Path> paths = ImmutableList.builder();
        if (input instanceof IMultiRequest) {
            for (Records.MultiOpRequest e: (IMultiRequest)input) {
                paths.addAll(apply(e));
            }
        } else if (input instanceof Records.PathGetter) {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) input).getPath());
            paths.add(path);
        }
        return paths.build();
    }
}