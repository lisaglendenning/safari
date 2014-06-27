package edu.uw.zookeeper.safari.schema;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.NameType;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.SimpleNameTrie;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.common.SameThreadExecutor;

public class PrefixCreator<V extends Operation.ProtocolResponse<?>> implements Callable<List<ListenableFuture<V>>> {

    public static <V extends Operation.ProtocolResponse<?>> PrefixCreator<V> forMaterializer(Materializer<?,V> materializer) {
        return new PrefixCreator<V>(materializer);
    }

    final static public Predicate<ValueNode<ZNodeSchema>> IS_PREFIX = new Predicate<ValueNode<ZNodeSchema>>() {
        @Override
        public boolean apply(ValueNode<ZNodeSchema> input) {
            return (NameType.STATIC == input.get().getNameType());
        }            
    };
    
    public static class PrefixIterator extends SimpleNameTrie.BreadthFirstTraversal<ValueNode<ZNodeSchema>> {
         
        public PrefixIterator(ValueNode<ZNodeSchema> root) {
            super(root);
        }

        @Override
        protected Iterable<ValueNode<ZNodeSchema>> childrenOf(ValueNode<ZNodeSchema> node) {
            return Iterables.filter(node.values(), IS_PREFIX);
        }
    };
    
    protected final Materializer<?,V> materializer;
    
    protected PrefixCreator(Materializer<?,V> materializer) {
        this.materializer = materializer;
    }
    
    @Override
    public List<ListenableFuture<V>> call() throws Exception {
        List<ListenableFuture<V>> futures = Lists.newLinkedList();
        PrefixCreator.PrefixIterator iterator = new PrefixIterator(materializer.schema().get().root());
        while (iterator.hasNext()) {
            ValueNode<ZNodeSchema> next = iterator.next();
            ListenableFuture<V> future = materializer.submit(Operations.Requests.exists().setPath(next.path()).build());
            if (! next.path().isRoot()) {
                future = Futures.transform(
                        future, 
                        new CreateIfAbsent(next), SameThreadExecutor.getInstance());
            }
            futures.add(future);
        }
        return futures;
    }

    protected static class NodeMayExist<V extends Operation.ProtocolResponse<?>> implements AsyncFunction<V,V> {
        @Override
        public ListenableFuture<V> apply(V result) throws KeeperException {
            Operations.maybeError(result.record(), result.toString(), KeeperException.Code.NODEEXISTS);
            return Futures.immediateFuture(result);
        }
    }
    
    protected class CreateIfAbsent implements AsyncFunction<V,V> {

        protected final ValueNode<ZNodeSchema> schema;
        
        public CreateIfAbsent(ValueNode<ZNodeSchema> schema) {
            this.schema = schema;
        }
        
        @Override
        public ListenableFuture<V> apply(V result) throws Exception {
            Optional<Operation.Error> error = Operations.maybeError(result.record(), result.toString(), KeeperException.Code.NONODE);
            if (error.isPresent()) {
                return Futures.transform(materializer.create(schema.path()).call(), new PrefixCreator.NodeMayExist<V>(), SameThreadExecutor.getInstance());
            } else {
                return Futures.immediateFuture(result);
            }
        }
    }
}