package edu.uw.zookeeper.safari.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public final class PathToQuery<I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> extends AbstractPair<I, ClientExecutor<? super Records.Request,O,?>> implements Function<ZNodePath, FixedQuery<O>> {

    public static <O extends Operation.ProtocolResponse<?>> PathToQuery<PathToRequestList, O> forRequests(ClientExecutor<? super Records.Request,O,?> client, Operations.PathBuilder<? extends Records.Request,?>...requests) {
        return forFunction(client, new PathToRequestList(ImmutableList.copyOf(requests)));
    }
    
    public static <O extends Operation.ProtocolResponse<?>> PathToQuery<PathToRequestList, O> forRequests(ClientExecutor<? super Records.Request,O,?> client, Iterable<? extends Operations.PathBuilder<? extends Records.Request,?>> requests) {
        return forFunction(client, new PathToRequestList(ImmutableList.copyOf(requests)));
    }

    public static <I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> PathToQuery<I, O> forFunction(ClientExecutor<? super Records.Request,O,?> client, I requests) {
        return new PathToQuery<I, O>(requests, client);
    }
    
    public PathToQuery(I requests, ClientExecutor<? super Records.Request,O,?> client) {
        super(checkNotNull(requests), checkNotNull(client));
    }
    
    public I requests() {
        return first;
    }
    
    public ClientExecutor<? super Records.Request,O,?> client() {
        return second;
    }
    
    @Override
    public FixedQuery<O> apply(final ZNodePath path) {
        return FixedQuery.forRequests(client(), requests().apply(path));
    }
    
    public static class PathToRequestList implements Function<ZNodePath, List<Records.Request>> {

        private final ImmutableList<Operations.PathBuilder<? extends Records.Request,?>> requests;
        
        public PathToRequestList(ImmutableList<Operations.PathBuilder<? extends Records.Request,?>> requests) {
            this.requests = requests;
        }
        
        public ImmutableList<Operations.PathBuilder<? extends Records.Request,?>> requests() {
            return requests;
        }
        
        @Override
        public List<Records.Request> apply(final ZNodePath path) {
            List<Records.Request> requests = Lists.newArrayListWithCapacity(requests().size());
            for (Operations.PathBuilder<? extends Records.Request,?> request: requests()) {
                requests.add(request.setPath(path).build());
            }
            return requests;
        }
    }
}