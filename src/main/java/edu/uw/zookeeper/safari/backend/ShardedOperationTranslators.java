package edu.uw.zookeeper.safari.backend;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.common.CachedFunction;
import edu.uw.zookeeper.safari.data.Volume;

public class ShardedOperationTranslators implements AsyncFunction<Identifier, OperationPrefixTranslator> {

    public static ShardedOperationTranslators of(CachedFunction<Identifier, Volume> lookup) {
        return new ShardedOperationTranslators(lookup);
    }
    
    public static ZNodeLabel.Path rootOf(Identifier id) {
        return (ZNodeLabel.Path) ZNodeLabel.joined(BackendSchema.Volumes.path(), id);
    }
    
    public static ZNodeLabel.Path pathOf(Identifier id, Object path) {
        return (ZNodeLabel.Path) ZNodeLabel.joined(rootOf(id), path);
    }
    
    protected static final Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();
    
    protected final VolumeToTranslator constructor;
    protected final CachedFunction<Identifier, Volume> lookup;
    
    public ShardedOperationTranslators(CachedFunction<Identifier, Volume> lookup) {
        this.constructor = new VolumeToTranslator();
        this.lookup = lookup;
    }

    @Override
    public ListenableFuture<OperationPrefixTranslator> apply(Identifier input)
            throws Exception {
        if (input.equals(Identifier.zero())) {
            return Futures.immediateFuture(constructor.apply(Volume.none()));
        }
        return Futures.transform(
                lookup.apply(input), constructor, sameThreadExecutor);
    }

    public static class VolumeToTranslator implements Function<Volume, OperationPrefixTranslator> {
    
        public VolumeToTranslator() {}
        
        @Override
        public OperationPrefixTranslator apply(Volume input) {
            checkNotNull(input);
            if (input.equals(Volume.none())) {
                return OperationPrefixTranslator.create(
                        RecordPrefixTranslator.<Records.Request>none(), 
                        RecordPrefixTranslator.<Records.Response>none());
            } else {
                return OperationPrefixTranslator.create(
                        input.getDescriptor().getRoot(), 
                        rootOf(input.getId()));
            }
        }
    }
}
