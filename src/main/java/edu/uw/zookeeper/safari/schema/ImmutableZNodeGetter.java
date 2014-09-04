package edu.uw.zookeeper.safari.schema;

import java.util.List;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.FutureCallback;

import edu.uw.zookeeper.client.PathToQuery;
import edu.uw.zookeeper.client.PathToRequests;
import edu.uw.zookeeper.client.Watchers;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.data.JoinToPath;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.safari.schema.DirectoryEntryListener;

/**
 * Queries an child znode with immutable data when the parent znode is added to the cache.
 * 
 * Assumes that the child is created atomically with the parent.
 */
public final class ImmutableZNodeGetter<U extends SafariZNode<U,?>, T extends U> implements FutureCallback<ZNodePath> {
    
    public static <O extends Operation.ProtocolResponse<?>, U extends SafariZNode<U,?>, T extends U> Watchers.FutureCallbackListener<?> listen(
            Class<T> type,
            Materializer<U,O> materializer,
            DirectoryEntryListener<U, ?> service) {
        final ImmutableZNodeGetter<U,T> instance = 
                create(type, materializer,
                        Watchers.MaybeErrorProcessor.maybeNoNode(), 
                        Watchers.FailWatchListener.create(service));
        return DirectoryEntryListener.entryCreatedCallback(
                Watchers.EventToPathCallback.create(instance), service);
    }

    public static <O extends Operation.ProtocolResponse<?>, V, U extends SafariZNode<U,?>, T extends U> ImmutableZNodeGetter<U,T> create(
            Class<T> type,
            Materializer<U,O> materializer,
            Processor<? super List<O>, V> processor,
            FutureCallback<? super V> callback) {
        ValueNode<ZNodeSchema> schema = materializer.schema().apply(type);
        @SuppressWarnings("unchecked")
        Watchers.PathToQueryCallback<O,V> query = Watchers.PathToQueryCallback.create(PathToQuery.forFunction(
                materializer,
                Functions.compose(
                        PathToRequests.forRequests(
                                Operations.Requests.sync(),
                                Operations.Requests.getData()),
                        JoinToPath.forName(
                                schema.parent().name()))), 
            processor, callback);
        return new ImmutableZNodeGetter<U,T>(
                schema,
                query,
                materializer.cache());
    }
    
    private final FutureCallback<ZNodePath> callback;
    private final LockableZNodeCache<U,?,?> cache;
    private final ValueNode<ZNodeSchema> schema;
    
    protected ImmutableZNodeGetter(
            ValueNode<ZNodeSchema> schema,
            FutureCallback<ZNodePath> callback,
            LockableZNodeCache<U,?,?> cache) {
        this.schema = schema;
        this.callback = callback;
        this.cache = cache;
    }
    
    public ValueNode<ZNodeSchema> schema() {
        return schema;
    }

    /**
     * Assumes cache is read locked.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void onSuccess(ZNodePath result) {
        U parent = (U) cache.cache().get(result);
        T child = (T) parent.get(schema.parent().name());
        if ((child == null) || (child.data().stamp() < 0L)) {
            callback.onSuccess(result);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        callback.onFailure(t);
    }
}
