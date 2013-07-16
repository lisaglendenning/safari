package edu.uw.zookeeper.orchestra.frontend;

import java.util.List;
import java.util.Map;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabel.Path;
import edu.uw.zookeeper.orchestra.VolumeAssignment;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class LookupRequests {

    public static ListenableFuture<Map<ZNodeLabel.Path, VolumeAssignment>> lookup(
            Records.Request request,
            AsyncFunction<ZNodeLabel.Path, VolumeAssignment> lookup) {
        switch (request.getOpcode()) {
        case CREATE:
        case CREATE2:
        case DELETE:
        {
            try {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) request).getPath());
                ListenableFuture<Map<ZNodeLabel.Path, VolumeAssignment>> future = 
                        Futures.transform(lookup.apply(path), TransformLookup.of(path));
                if (path.isRoot()) {
                    return future;
                } else {
                    ZNodeLabel.Path parent = (ZNodeLabel.Path) path.head();
                    return Futures.transform(
                            Futures.allAsList(
                                    ImmutableList.of(
                                            future,
                                            Futures.transform(
                                                    lookup.apply(parent), 
                                                    TransformLookup.of(parent)))), 
                            TransformLookups.UNION);
                }
            } catch (Exception e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
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
            try {
                ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) request).getPath());
                return Futures.transform(lookup.apply(path), TransformLookup.of(path));
            } catch (Exception e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
        case MULTI:
        {
            try {
                List<ListenableFuture<Map<ZNodeLabel.Path, VolumeAssignment>>> futures = Lists.newLinkedList();
                for (Records.MultiOpRequest e: (IMultiRequest) request) {
                    futures.add(lookup(e, lookup));
                }
                return Futures.transform(
                        Futures.allAsList(futures), 
                        TransformLookups.UNION);
            } catch (Exception e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
        case SET_WATCHES:
            // TODO
            throw new UnsupportedOperationException();
        default:
            return Futures.<Map<ZNodeLabel.Path, VolumeAssignment>>immediateFuture(
                    ImmutableMap.<ZNodeLabel.Path, VolumeAssignment>of());
        }
    }

    protected static class TransformLookup implements Function<VolumeAssignment, Map<ZNodeLabel.Path, VolumeAssignment>> {
    
        public static TransformLookup of(ZNodeLabel.Path path) {
            return new TransformLookup(path);
        }
        
        protected final ZNodeLabel.Path path;
        
        public TransformLookup(ZNodeLabel.Path path) {
            this.path = path;
        }
    
        @Override
        public Map<Path, VolumeAssignment> apply(VolumeAssignment input) {
            return ImmutableMap.of(path, input);
        }
    }

    protected static enum TransformLookups implements Function<List<Map<ZNodeLabel.Path, VolumeAssignment>>, Map<ZNodeLabel.Path, VolumeAssignment>> {
        UNION;
    
        @Override
        public Map<Path, VolumeAssignment> apply(List<Map<ZNodeLabel.Path, VolumeAssignment>> input) {
            Map<Path, VolumeAssignment> combined = Maps.newHashMapWithExpectedSize(input.size());
            for (Map<ZNodeLabel.Path, VolumeAssignment> e: input) {
                combined.putAll(e);
            }
            return ImmutableMap.copyOf(combined);
        }
    }
}