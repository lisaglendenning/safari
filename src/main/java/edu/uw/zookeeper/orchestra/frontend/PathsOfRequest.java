package edu.uw.zookeeper.orchestra.frontend;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public enum PathsOfRequest implements Function<Records.Request, ZNodeLabel.Path[]> {
    PATHS_OF_REQUEST;
    
    public static ZNodeLabel.Path[] getPathsOfRequest(Records.Request request) {
        return PATHS_OF_REQUEST.apply(request);
    }

    @Override
    public ZNodeLabel.Path[] apply(Records.Request input) {
        ZNodeLabel.Path[] paths;
        switch (input.opcode()) {
        case CREATE:
        case CREATE2:
        case DELETE:
        {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) input).getPath());
            if (path.isRoot()) {
                paths = new ZNodeLabel.Path[1];
                paths[0] = path;
            } else {
                paths = new ZNodeLabel.Path[2];
                paths[0] = path;
                paths[1] = (ZNodeLabel.Path) path.head();
            }
            break;
        }
        case CHECK:
        case EXISTS:
        case GET_ACL:
        case GET_CHILDREN:
        case GET_CHILDREN2:
        case GET_DATA:
        case SET_ACL:
        case SET_DATA:
        case SYNC:
        {
            paths = new ZNodeLabel.Path[1];
            paths[0] = ZNodeLabel.Path.of(((Records.PathGetter) input).getPath());
            break;
        }
        case MULTI:
        {
            ImmutableSet.Builder<ZNodeLabel.Path> builder = ImmutableSet.builder();
            for (Records.MultiOpRequest e: (IMultiRequest) input) {
                for (ZNodeLabel.Path p: apply(e)) {
                    builder.add(p);
                }
            }
            ImmutableSet<ZNodeLabel.Path> built = builder.build();
            paths = built.toArray(new ZNodeLabel.Path[built.size()]);
            break;
        }
        case SET_WATCHES:
            // TODO
            throw new UnsupportedOperationException();
        default:
            paths = new ZNodeLabel.Path[0];
            break;
        }
        return paths;
    }
}
