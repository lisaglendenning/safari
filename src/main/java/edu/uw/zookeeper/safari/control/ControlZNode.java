package edu.uw.zookeeper.safari.control;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.CallablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.data.Name;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.NameType;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.ValueNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeSchema;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.safari.data.LookupHashedTask;
import edu.uw.zookeeper.safari.data.SafariZNode;


public abstract class ControlZNode<V> extends SafariZNode<ControlZNode<?>,V> {

    protected ControlZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            NameTrie.Pointer<? extends ControlZNode<?>> parent) {
        this(schema, codec, null, null, -1L, parent);
    }

    protected ControlZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends ControlZNode<?>> parent) {
        super(schema, codec, data, stat, stamp, parent);
    }

    protected ControlZNode(
            ValueNode<ZNodeSchema> schema,
            Serializers.ByteCodec<Object> codec,
            V data,
            Records.ZNodeStatGetter stat,
            long stamp,
            NameTrie.Pointer<? extends ControlZNode<?>> parent,
            Map<ZNodeName, ControlZNode<?>> children) {
        super(schema, codec, data, stat, stamp, parent, children);
    }

    public static abstract class ControlNamedZNode<V,T> extends ControlZNode<V> {

        protected final T name;

        protected ControlNamedZNode(
                T name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            this(name, schema, codec, null, null, -1L, parent);
        }

        protected ControlNamedZNode(
                T name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            super(schema, codec, data, stat, stamp, parent);
            this.name = name;
        }

        public T name() {
            return name;
        }
    }
    
    public static abstract class IdentifierZNode extends ControlNamedZNode<Void,Identifier> {

        @Name(type=NameType.PATTERN)
        public static final ZNodeLabel LABEL = ZNodeLabel.fromString(Identifier.PATTERN);
        
        protected IdentifierZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            this(Identifier.valueOf(parent.name().toString()), schema, codec, parent);
        }
        
        protected IdentifierZNode(
                Identifier name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            this(name, schema, codec, null, -1L, parent);
        }

        protected IdentifierZNode(
                Identifier name,
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            super(name, schema, codec, null, stat, stamp, parent);
        }
    }
    
    public static abstract class ControlEntityDirectoryZNode<V,T extends ControlZNode<V>,U extends IdentifierZNode> extends ControlZNode<Void> {
    
        protected ControlEntityDirectoryZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            this(schema, codec, null, -1L, parent);
        }
    
        protected ControlEntityDirectoryZNode(
                ValueNode<ZNodeSchema> schema,
                Serializers.ByteCodec<Object> codec,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends ControlZNode<?>> parent) {
            super(schema, codec, null, stat, stamp, parent);
        }

        public abstract Class<? extends U> entityType();

        public abstract Class<? extends T> hashedType();
        
        public abstract Function<V, Hash.Hashed> hasher();
    }

    public static class LookupEntity<O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>,U extends IdentifierZNode,C extends ControlEntityDirectoryZNode<V,T,U>> implements Supplier<V>, AsyncFunction<Identifier, Optional<Identifier>> {

        public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends IdentifierZNode,C extends ControlEntityDirectoryZNode<V,T,U>>
        LookupHashedTask<Identifier> call(
                ZNodePath directory,
                V value,
                Materializer<ControlZNode<?>, O> materializer) {
            LookupHashedTask<Identifier> task = LookupHashedTask.create(
                    hash(value, directory, materializer),
                    LookupEntity.newInstance(value, directory, materializer),
                    SettableFuturePromise.<Identifier>create());
            return task;
        }
        
        @SuppressWarnings("unchecked")
        public static <V,T extends ControlZNode<V>,U extends IdentifierZNode,C extends ControlEntityDirectoryZNode<V,T,U>> Hash.Hashed hash(
                V value,
                ZNodePath directory,
                Materializer<ControlZNode<?>, ?> materializer) {
            materializer.cache().lock().readLock().lock();
            try {
                return ((C) materializer.cache().cache().get(directory)).hasher().apply(value);
            } finally {
                materializer.cache().lock().readLock().unlock();
            }
        }

        public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends IdentifierZNode,C extends ControlEntityDirectoryZNode<V,T,U>>
        LookupEntity<O,V,T,U,C> newInstance(
                V value,
                ZNodePath directory,
                Materializer<ControlZNode<?>, O> materializer) {
            return new LookupEntity<O,V,T,U,C>(value, directory, materializer);
        }

        protected final V value;
        protected final ZNodePath directory;
        protected final Materializer<ControlZNode<?>,O> materializer;
        
        public LookupEntity(
                V value,
                ZNodePath directory,
                Materializer<ControlZNode<?>, O> materializer) {
            this.value = checkNotNull(value);
            this.materializer = checkNotNull(materializer);
            this.directory = checkNotNull(directory);
        }
        
        public Materializer<ControlZNode<?>, O> materializer() {
            return materializer;
        }

        public ZNodePath directory() {
            return directory;
        }
        
        @Override
        public V get() {
            return value;
        }
        
        @Override
        public ListenableFuture<Optional<Identifier>> apply(final Identifier id) {
            final ZNodePath path;
            materializer.cache().lock().readLock().lock();
            try {
                @SuppressWarnings("unchecked")
                C directory = (C) materializer.cache().cache().get(directory());
                path = directory.path().join(
                        ZNodeLabel.fromString(id.toString())).join(
                                materializer.schema().apply(directory.hashedType()).parent().name());
            } finally {
                materializer.cache().lock().readLock().unlock();
            }
            return Futures.transform(
                    materializer.getData(path).call(), 
                    new Listener(id, path),
                    SameThreadExecutor.getInstance());
        }
        
        protected class Listener implements Function<O, Optional<Identifier>> {

            private final Identifier id;
            private final ZNodePath path;
            
            public Listener(Identifier id, ZNodePath path) {
                this.id = id;
                this.path = path;
            }
            
            @Override
            public @Nullable
            Optional<Identifier> apply(O input) {
                if (input.record() instanceof Operation.Error) {
                    return Optional.absent();
                } else {
                    materializer.cache().lock().readLock().lock();
                    try {
                        @SuppressWarnings("unchecked")
                        T node = (T) materializer.cache().cache().get(path);
                        if (Objects.equal(node.data().get(), get())) {
                            return Optional.of(id);
                        } else {
                            return Optional.absent();
                        }
                    } finally {
                        materializer.cache().lock().readLock().unlock();
                    }
                }
            }
        }
    }

    public static class CreateEntity<O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends IdentifierZNode,C extends ControlEntityDirectoryZNode<V,T,U>> implements Supplier<LookupEntity<O,V,T,U,C>>, AsyncFunction<Identifier, Optional<Identifier>> {

        public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends IdentifierZNode,C extends ControlEntityDirectoryZNode<V,T,U>>
        LookupHashedTask<Identifier> call(
                ZNodePath directory,
                V value,
                Materializer<ControlZNode<?>, O> materializer) {
            LookupHashedTask<Identifier> task = LookupHashedTask.create(
                    LookupEntity.hash(value, directory, materializer),
                    CreateEntity.newInstance(LookupEntity.newInstance(value, directory, materializer)),
                    SettableFuturePromise.<Identifier>create());
            return task;
        }

        public static <O extends Operation.ProtocolResponse<?>,V,T extends ControlZNode<V>, U extends IdentifierZNode,C extends ControlEntityDirectoryZNode<V,T,U>>
        CreateEntity<O,V,T,U,C> newInstance(
                LookupEntity<O,V,T,U,C> lookup) {
            return new CreateEntity<O,V,T,U,C>(lookup);
        }
        
        protected final LookupEntity<O,V,T,U,C> lookup;

        public CreateEntity(
                LookupEntity<O,V,T,U,C> lookup) {
            this.lookup = lookup;
        }
        
        @Override
        public LookupEntity<O,V,T,U,C> get() {
            return lookup;
        }
        
        @Override
        public ListenableFuture<Optional<Identifier>> apply(Identifier input) {
            CreateHashedEntityTask task = new CreateHashedEntityTask(
                    input, 
                    LoggingPromise.create(LogManager.getLogger(this), SettableFuturePromise.<Optional<Identifier>>create()));
            task.run();
            return task;
        }

        protected class CreateHashedEntityTask extends PromiseTask<Identifier, Optional<Identifier>> implements Runnable, Callable<Optional<Optional<Identifier>>> {
        
            protected final CallablePromiseTask<CreateHashedEntityTask, Optional<Identifier>> delegate;
            protected Optional<ListenableFuture<O>> createFuture;
            protected Optional<ListenableFuture<Optional<Identifier>>> valueFuture;
            
            public CreateHashedEntityTask(
                    Identifier task,
                    Promise<Optional<Identifier>> promise) {
                super(task, promise);
                this.delegate = CallablePromiseTask.create(this, this);
                this.createFuture = Optional.absent();
                this.valueFuture = Optional.absent();
                addListener(this, SameThreadExecutor.getInstance());
            }

            @Override
            public synchronized void run() {
                if (isDone()) {
                    if (isCancelled()) {
                        if (createFuture.isPresent()) {
                            createFuture.get().cancel(false);
                        }
                        if (valueFuture.isPresent()) {
                            valueFuture.get().cancel(false);
                        }
                    }
                } else {
                    delegate.run();
                }
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public synchronized Optional<Optional<Identifier>> call() throws Exception {
                if (!createFuture.isPresent()) {
                    ZNodePath path = lookup.directory().join(
                            ZNodeLabel.fromString(task().toString()));
                    Class<? extends T> hashedType;
                    lookup.materializer().cache().lock().readLock().lock();
                    try {
                        hashedType = ((C) lookup.materializer().cache().cache().get(lookup.directory())).hashedType();
                    } finally {
                        lookup.materializer().cache().lock().readLock().unlock();
                    }
                    createFuture = Optional.of(lookup.materializer().cache().submit(
                            Operations.Requests.multi()
                                .add(lookup.materializer().create(path).get())
                                .add(lookup.materializer().create(
                                        path.join(lookup.materializer().schema().apply(hashedType).parent().name()), 
                                        CreateEntity.this.get().get()).get())
                                .build()));
                    createFuture.get().addListener(this, SameThreadExecutor.getInstance());
                    return Optional.absent();
                }
                if (! createFuture.get().isDone()) {
                    return Optional.absent();
                } else if (createFuture.get().isCancelled()) {
                    throw new CancellationException();
                }
        
                if (!valueFuture.isPresent()) {
                    IMultiResponse response = (IMultiResponse) Operations.unlessError(createFuture.get().get().record());
                    Operation.Error error = null;
                    for (Records.MultiOpResponse e: response) {
                        if (e instanceof Operation.Error) {
                            error = (Operation.Error) e;
                            switch (error.error()) {
                            case OK:
                            case NODEEXISTS:
                            case RUNTIMEINCONSISTENCY:
                                break;
                            default:
                                throw KeeperException.create(error.error());
                            }
                        }
                    }
                    if (error == null) {
                        // success!
                        return Optional.of(Optional.of(task()));
                    }
                    
                    // check if the existing node is my value
                    valueFuture = Optional.of(lookup.apply(task()));
                    valueFuture.get().addListener(this, SameThreadExecutor.getInstance());
                    return Optional.absent();
                }
                if (! valueFuture.get().isDone()) {
                    return Optional.absent();
                } else if (valueFuture.get().isCancelled()) {
                    throw new CancellationException();
                }
                
                try {
                    return Optional.of(valueFuture.get().get());
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KeeperException.NoNodeException) {
                        // hmm...try again?
                        valueFuture = Optional.absent();
                        createFuture = Optional.absent();
                        run();
                    } else {
                        throw e;
                    }
                    return Optional.absent();
                }
            }
        }
    }
}