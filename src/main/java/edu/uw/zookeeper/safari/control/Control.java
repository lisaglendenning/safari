package edu.uw.zookeeper.safari.control;


import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.client.TreeFetcher.Parameters;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.RunnablePromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.safari.Hash;
import edu.uw.zookeeper.safari.Identifier;

public abstract class Control {

    public static ZNodeLabel.Path path(Object element) {
        return ControlZNode.path(element);
    }

    public static ZNodeLabel.Path path(Object parent, Object element) {
        return ControlZNode.path(parent, element);
    }

    public static ZNodeLabel label(Object element) {
        return ControlZNode.label(element);
    }
    
    public static void createPrefix(Materializer<?> materializer) throws InterruptedException, ExecutionException, KeeperException {
        // The prefix is small enough that there's no need to get fancy here
        final Predicate<Schema.SchemaNode> isPrefix = new Predicate<Schema.SchemaNode>() {
            @Override
            public boolean apply(Schema.SchemaNode input) {
                return (LabelType.LABEL == input.get().getLabelType());
            }            
        };
        
        final Iterator<Schema.SchemaNode> iterator = new ZNodeLabelTrie.BreadthFirstTraversal<Schema.SchemaNode>(materializer.schema().root()) {
            @Override
            protected Iterable<Schema.SchemaNode> childrenOf(Schema.SchemaNode node) {
                return Iterables.filter(node.values(), isPrefix);
            }
        };
        
        while (iterator.hasNext()) {
            Schema.SchemaNode node = iterator.next();
            Operation.ProtocolResponse<?> result = materializer.operator().exists(node.path()).submit().get();
            Optional<Operation.Error> error = Operations.maybeError(result.record(), KeeperException.Code.NONODE, result.toString());
            if (error.isPresent()) {
                result = materializer.operator().create(node.path()).submit().get();
                error = Operations.maybeError(result.record(), KeeperException.Code.NODEEXISTS, result.toString());
            }
        }
    }

    @ZNode(acl=Acls.Definition.ANYONE_ALL)
    public static abstract class ControlZNode {
        
        public static Schema.SchemaNode schemaNode(Object element) {
            return ControlSchema.getInstance().byElement(element);
        }

        public static ZNodeLabel label(Object element) {
            if (element instanceof ControlZNode) {
                return ((ControlZNode) element).label();
            } else {
                return schemaNode(element).parent().get().label();
            }
        }

        public static ZNodeLabel.Path path(Object parent, Object element) {
            return (ZNodeLabel.Path) ZNodeLabel.joined(path(parent), label(element));
        }

        public static ZNodeLabel.Path path(Object element) {
            if (element instanceof ControlZNode) {
                return ((ControlZNode) element).path();
            } else {
                return schemaNode(element).path();
            }
        }

        private final Object parent;
        private final ZNodeLabel.Path path;
        private final ZNodeLabel label;

        protected ControlZNode() {
            this(null);
        }

        protected ControlZNode(Object parent) {
            this(parent, null);
        }
        
        protected ControlZNode(
                Object parent,
                ZNodeLabel label) {
            this.parent = (parent == null) ? getClass().getEnclosingClass() : parent;
            this.label = (label == null) ? label(getClass()) : label;
            this.path = (ZNodeLabel.Path) ZNodeLabel.joined(path(this.parent), this.label);
        }
        
        public Object parent() {
            return parent;
        }

        public ZNodeLabel label() {
            return label;
        }

        public ZNodeLabel.Path path() {
            return path;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).addValue(path().toString()).toString();
        }
    }

    public static abstract class ValueZNode<T> extends ControlZNode implements Reference<T> {

        @SuppressWarnings("unchecked")
        public static <T, C extends ValueZNode<T>> C newInstance(Class<C> cls, T value) {
            for (Constructor<?> c: cls.getDeclaredConstructors()) {
                Class<?>[] parameterTypes = c.getParameterTypes();
                if ((parameterTypes.length == 1) 
                        && parameterTypes[0].isAssignableFrom(value.getClass())) {
                    try {
                        return (C) c.newInstance(value);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }
            throw new AssertionError(Arrays.toString(cls.getDeclaredConstructors()));
        }
        
        @SuppressWarnings("unchecked")
        public static <T, C extends ValueZNode<T>> C newInstance(Class<C> cls, T value, Object parent) {
            for (Constructor<?> c: cls.getDeclaredConstructors()) {
                Class<?>[] parameterTypes = c.getParameterTypes();
                if ((parameterTypes.length == 2) 
                        && ((value == null) || parameterTypes[0].isAssignableFrom(value.getClass()))
                        && ((parent == null) || parameterTypes[1].isAssignableFrom(parent.getClass()))) {
                    try {
                        return (C) c.newInstance(value, parent);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }
            throw new AssertionError(String.format("Unable to construct %s from (%s, %s)", cls, value, parent));
        }

        @SuppressWarnings("unchecked")
        public static <T, C extends ValueZNode<T>> ListenableFuture<C> getValue(
                final Class<C> cls, 
                final Object parent, 
                final Materializer<?> materializer) {
            final ZNodeLabel.Path path = path(parent, cls);
            final Callable<Optional<C>> creator = new Callable<Optional<C>>() {
                @Override
                public Optional<C> call() {
                    Materializer.MaterializedNode node = materializer.get(path);
                    if (node != null) {
                        StampedReference<?> stamped = node.get();
                        if (stamped != null) {
                            T value = (T) stamped.get();
                            if (value != null) {
                                return Optional.of(newInstance(cls, value, parent));
                            }
                        }
                    }
                    return Optional.absent();
                }
            };
            try {
                Optional<C> result = creator.call();
                if (result.isPresent()) {
                    return Futures.immediateFuture(result.get());
                }
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
            return Futures.transform(
                    materializer.operator().getData(path).submit(),
                    new AsyncFunction<Operation.ProtocolResponse<?>, C>() {
                        @Override
                        public @Nullable
                        ListenableFuture<C> apply(Operation.ProtocolResponse<?> input) throws Exception {
                            Operations.unlessError(input.record());
                            Optional<C> result = creator.call();               
                            if (result.isPresent()) {
                                return Futures.immediateFuture(result.get());
                            } else {
                                // dunno
                                throw new AssertionError();
                            }
                        }
                    });
        }
        
        public static <T, C extends ValueZNode<T>> ListenableFuture<C> create(
                final Class<C> cls, 
                final T value, 
                final Object parent, 
                final Materializer<?> materializer) {
            final C instance = newInstance(cls, value, parent);
            final ZNodeLabel.Path path = instance.path();
            return Futures.transform(
                    materializer.operator().create(path, value).submit(),
                    new AsyncFunction<Operation.ProtocolResponse<?>, C>() {
                        @Override
                        public @Nullable
                        ListenableFuture<C> apply(Operation.ProtocolResponse<?> input) throws KeeperException {
                            Optional<Operation.Error> error = Operations.maybeError(input.record(), KeeperException.Code.NODEEXISTS, input.toString());
                            if (error.isPresent()) {
                                return getValue(cls, parent, materializer);
                            } else {
                                return Futures.immediateFuture(instance);
                            }
                        }
                    });
        }
        
        protected final T value;
    
        protected ValueZNode(T value) {
            this(value, null);
        }

        protected ValueZNode(T value, Object parent) {
            this(value, parent, null);
        }
        
        protected ValueZNode(T value, Object parent, ZNodeLabel label) {
            super(parent, label);
            this.value = value;
        }
        
        @Override
        public T get() {
            return value;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .addValue(path().toString()).addValue(get()).toString();
        }
    }
    
    public static abstract class IdentifierZNode extends ValueZNode<Identifier> {
        
        public static <C extends IdentifierZNode> ListenableFuture<C> getIdentifier(
                final Class<C> cls, 
                final Object parent, 
                final Materializer<?> materializer) {
            final ZNodeLabel.Path path = path(parent, cls);
            final Callable<Optional<C>> creator = new Callable<Optional<C>>() {
                @Override
                public Optional<C> call() {
                    Materializer.MaterializedNode node = materializer.get(path);
                    if (node != null) {
                        Identifier value = Identifier.valueOf(node.parent().get().label().toString());
                        return Optional.of(newInstance(cls, value, parent));
                    }
                    return Optional.absent();
                }
            };
            try {
                Optional<C> result = creator.call();
                if (result.isPresent()) {
                    return Futures.immediateFuture(result.get());
                }
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
            return Futures.transform(
                    materializer.operator().getData(path).submit(),
                    new AsyncFunction<Operation.ProtocolResponse<?>, C>() {
                        @Override
                        public @Nullable
                        ListenableFuture<C> apply(Operation.ProtocolResponse<?> input) throws Exception {
                            Operations.unlessError(input.record());
                            Optional<C> result = creator.call();               
                            if (result.isPresent()) {
                                return Futures.immediateFuture(result.get());
                            } else {
                                // dunno
                                throw new AssertionError();
                            }
                        }
                    });
        }
        
        public static <T, C extends IdentifierZNode> ListenableFuture<C> create(
                final Class<C> cls, 
                final Identifier value, 
                final Object parent, 
                final Materializer<?> materializer) {
            final C instance = newInstance(cls, value, parent);
            final ZNodeLabel.Path path = instance.path();
            return Futures.transform(
                    materializer.operator().create(path).submit(),
                    new AsyncFunction<Operation.ProtocolResponse<?>, C>() {
                        @Override
                        public @Nullable
                        ListenableFuture<C> apply(Operation.ProtocolResponse<?> input) throws KeeperException {
                            Optional<Operation.Error> error = Operations.maybeError(input.record(), KeeperException.Code.NODEEXISTS, input.toString());
                            if (error.isPresent()) {
                                return getIdentifier(cls, parent, materializer);
                            } else {
                                return Futures.immediateFuture(instance);
                            }
                        }
                    });
        }
        
        protected IdentifierZNode(Identifier value) {
            this(value, null);
        }
        
        protected IdentifierZNode(Identifier value, Object parent) {
            super(value, parent, ZNodeLabel.Component.of(value.toString()));
        }
    }

    public static class EntityValue<V, T extends IdentifierZNode, U extends ValueZNode<V>> {
        
        public static <V, T extends IdentifierZNode, U extends ValueZNode<V>> EntityValue<V,T,U> create(
                Class<T> entityType, Class<U> valueType) {
            return new EntityValue<V,T,U>(entityType, valueType);
        }
            
        protected final Class<T> entityType;
        protected final Class<U> valueType;
        
        public EntityValue(Class<T> entityType, Class<U> valueType) {
            super();
            this.entityType = entityType;
            this.valueType = valueType;
        }
    
        public Class<T> getEntityType() {
            return entityType;
        }
    
        public Class<U> getValueType() {
            return valueType;
        }
    }

    public static class FetchUntil<V> extends PromiseTask<TreeFetcher.Builder<V>, V> implements FutureCallback<Optional<V>> {
    
        public static <V> FetchUntil<V> newInstance(
                ZNodeLabel.Path root, 
                Processor<? super Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<V>> result, 
                Materializer<?> materializer) {
            TreeFetcher.Builder<V> fetcher = TreeFetcher.<V>builder()
                    .setParameters(Parameters.of(true, true, false, false))
                    .setResult(result).setClient(materializer).setRoot(root);
            Promise<V> promise = newPromise();
            return newInstance(root, materializer, fetcher, promise);
        }
    
        public static <V> FetchUntil<V> newInstance(
                ZNodeLabel.Path root, Materializer<?> materializer, TreeFetcher.Builder<V> fetcher, Promise<V> promise) {
            return new FetchUntil<V>(
                    root, materializer, fetcher, promise);
        }

        public static <V> Promise<V> newPromise() {
            return LoggingPromise.create(LogManager.getLogger(FetchUntil.class), PromiseTask.<V>newPromise());
        }
    
        protected final ZNodeLabel.Path root;
        protected final Materializer<?> materializer;
        
        public FetchUntil(
                ZNodeLabel.Path root, 
                Materializer<?> materializer, 
                TreeFetcher.Builder<V> fetcher,
                Promise<V> promise) {
            super(fetcher, promise);
            this.root = root;
            this.materializer = materializer;
            
            materializer.register(this);
            materializer.operator().sync(root).submit();
            new Updater(root);
        }
    
        @Subscribe
        public void handleReply(Operation.ProtocolResponse<?> message) {
            if (OpCodeXid.NOTIFICATION.xid() == message.xid()) {
                WatchEvent event = WatchEvent.fromRecord((IWatcherEvent) message.record());
                ZNodeLabel.Path path = event.getPath();
                if (root.prefixOf(path)) {
                    new Updater(path);
                }
            }
        }
    
        @Override
        public void onSuccess(Optional<V> result) {
            try {
                if (result.isPresent()) {
                    set(result.get());
                } else {
                    // force watches
                    materializer.operator().sync(root).submit();
                }
            } catch (Exception e) {
                setException(e);
            }
        }
    
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean isSet = super.cancel(mayInterruptIfRunning);
            if (isSet) {
                try {
                    materializer.unregister(this);
                } catch (IllegalArgumentException e) {}
            }
            return isSet;
        }
        
        @Override
        public boolean set(V result) {
            boolean isSet = super.set(result);
            if (isSet) {
                try {
                    materializer.unregister(this);
                } catch (IllegalArgumentException e) {}
            }
            return isSet;
        }
        
        @Override
        public boolean setException(Throwable t) {
            boolean isSet = super.setException(t);
            if (isSet) {
                try {
                    materializer.unregister(this);
                } catch (IllegalArgumentException e) {}
            }
            return isSet;
        }
        
        protected class Updater implements Runnable {
            protected final ListenableFuture<Optional<V>> future;
            
            public Updater(ZNodeLabel.Path path) {
                this.future = task().setRoot(path).build();
                Futures.addCallback(future, FetchUntil.this, MoreExecutors.sameThreadExecutor());
                FetchUntil.this.addListener(this, MoreExecutors.sameThreadExecutor());
            }
    
            @Override
            public void run() {
                future.cancel(true);
            }
        }
    }

    public static class LookupHashedTask<V> extends RunnablePromiseTask<AsyncFunction<Identifier, Optional<V>>, V> {
    
        public static <V> LookupHashedTask<V> create(
                Hash.Hashed hashed,
                AsyncFunction<Identifier, Optional<V>> lookup) {
            Promise<V> promise = SettableFuturePromise.create();
            return new LookupHashedTask<V>(hashed, lookup, promise);
        }
        
        protected ListenableFuture<Optional<V>> pending;
        protected Hash.Hashed hashed;
        
        public LookupHashedTask(
                Hash.Hashed hashed,
                AsyncFunction<Identifier, Optional<V>> lookup,
                Promise<V> promise) {
            super(lookup, promise);
            this.hashed = checkNotNull(hashed);
            this.pending = null;
        }

        @Override
        public synchronized boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancel = super.cancel(mayInterruptIfRunning);
            if (cancel) {
                if (pending != null) {
                    pending.cancel(mayInterruptIfRunning);
                }
            }
            return cancel;
        }

        @Override
        public synchronized Optional<V> call() throws Exception {
            if (pending == null) {
                Identifier id = hashed.asIdentifier();
                while (id.equals(Identifier.zero())) {
                    hashed = hashed.rehash();
                    id = hashed.asIdentifier();
                }
                pending = task().apply(id);
                pending.addListener(this, MoreExecutors.sameThreadExecutor());
                return Optional.absent();
            }
            
            if (!pending.isDone()) {
                return Optional.absent();
            } else if (pending.isCancelled()) {
                cancel(true);
                return Optional.absent();
            }
            
            Optional<V> result = pending.get();
            if (!result.isPresent()) {
                // try again
                hashed = hashed.rehash();
                pending = null;
                run();
            }
            return result;
        }
    }

    public static class CreateEntityTask<O extends Operation.ProtocolResponse<?>, V, T extends IdentifierZNode, U extends ValueZNode<V>> implements Reference<V>, AsyncFunction<Identifier, Optional<T>> {

        public static <O extends Operation.ProtocolResponse<?>, V, T extends IdentifierZNode, U extends ValueZNode<V>>
        CreateEntityTask<O,V,T,U> create(
                V value,
                EntityValue<V,T,U> schema,
                Materializer<O> materializer) {
            return new CreateEntityTask<O,V,T,U>(value, schema, materializer);
        }

        protected final V value;
        protected final EntityValue<V,T,U> schema;
        protected final Materializer<O> materializer;
        
        public CreateEntityTask(
                V value,
                EntityValue<V,T,U> schema,
                Materializer<O> materializer) {
            this.value = checkNotNull(value);
            this.materializer = checkNotNull(materializer);
            this.schema = checkNotNull(schema);
        }
        
        @Override
        public V get() {
            return value;
        }

        @Override
        public ListenableFuture<Optional<T>> apply(Identifier input) {
            CreateHashedEntityTask task = new CreateHashedEntityTask(
                    ValueZNode.newInstance(schema.getEntityType(), input), 
                    SettableFuturePromise.<Optional<T>>create());
            task.run();
            return task;
        }

        protected class CreateHashedEntityTask extends RunnablePromiseTask<T, Optional<T>> {
        
            protected ListenableFuture<O> createFuture;
            protected ListenableFuture<U> valueFuture;
            
            public CreateHashedEntityTask(
                    T entity,
                    Promise<Optional<T>> promise) {
                super(entity, promise);
                this.createFuture = null;
                this.valueFuture = null;
            }

            @Override
            public synchronized boolean cancel(boolean mayInterruptIfRunning) {
                boolean cancel = super.cancel(mayInterruptIfRunning);
                if (cancel) {
                    if (createFuture != null) {
                        createFuture.cancel(mayInterruptIfRunning);
                    }
                    if (valueFuture != null) {
                        valueFuture.cancel(mayInterruptIfRunning);
                    }
                }
                return cancel;
            }
            
            @Override
            public synchronized Optional<Optional<T>> call() throws Exception {
                if (createFuture == null) {
                    createFuture = materializer.submit(
                            Operations.Requests.multi()
                                .add(materializer.operator().create(
                                        task().path()).get())
                                .add(materializer.operator().create(
                                        path(task(), schema.getValueType()), 
                                        CreateEntityTask.this.get()).get())
                                .build());
                    createFuture.addListener(this, MoreExecutors.sameThreadExecutor());
                    return Optional.absent();
                }
                if (! createFuture.isDone()) {
                    return Optional.absent();
                } else if (createFuture.isCancelled()) {
                    cancel(true);
                    return Optional.absent();
                }
        
                if (valueFuture == null) {
                    IMultiResponse response = (IMultiResponse) Operations.unlessError(createFuture.get().record());
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
                    
                    valueFuture = ValueZNode.getValue(schema.getValueType(), task(), materializer);
                    valueFuture.addListener(this, MoreExecutors.sameThreadExecutor());
                    return Optional.absent();
                }
                if (! valueFuture.isDone()) {
                    return Optional.absent();
                } else if (valueFuture.isCancelled()) {
                    cancel(true);
                    return Optional.absent();
                }
                
                try {
                    V value = valueFuture.get().get();
                    assert (value != null);
                    if (CreateEntityTask.this.get().equals(value)) {
                        // success!
                        return Optional.of(Optional.of(task()));
                    } else {
                        // collision! try again...
                        return Optional.of(Optional.<T>absent());
                    }
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KeeperException.NoNodeException) {
                        // hmm...try again?
                        valueFuture = null;
                        createFuture = null;
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
