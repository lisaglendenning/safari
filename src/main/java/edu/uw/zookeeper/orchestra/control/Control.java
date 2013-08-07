package edu.uw.zookeeper.orchestra.control;


import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
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
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.data.Schema.LabelType;
import edu.uw.zookeeper.orchestra.common.Identifier;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Response;

public abstract class Control {

    public static ZNodeLabel.Path path(Object element) {
        return ControlZNode.path(element);
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
            Operation.ProtocolResponse<Records.Response> result = materializer.operator().exists(node.path()).submit().get();
            Optional<Operation.Error> error = Operations.maybeError(result.getRecord(), KeeperException.Code.NONODE, result.toString());
            if (error.isPresent()) {
                result = materializer.operator().create(node.path()).submit().get();
                error = Operations.maybeError(result.getRecord(), KeeperException.Code.NODEEXISTS, result.toString());
            }
        }
    }

    @ZNode(acl=Acls.Definition.ANYONE_ALL)
    public static abstract class ControlZNode {
        public static Schema.SchemaNode schemaNode(Object element) {
            return ControlSchema.getInstance().byElement(element);
        }
        
        public static ZNodeLabel.Path path(Object element) {
            if (element instanceof ControlZNode) {
                return ((ControlZNode) element).path();
            } else {
                return schemaNode(element).path();
            }
        }

        protected final Object parent;

        protected ControlZNode() {
            this(null);
        }
        
        protected ControlZNode(Object parent) {
            this.parent = (parent == null) ? getClass().getEnclosingClass() : parent;
        }
        
        public Object parent() {
            return parent;
        }

        public ZNodeLabel label() {
            return path(getClass()).tail();
        }

        public ZNodeLabel.Path path() {
            return ZNodeLabel.Path.of(path(parent()), label());
        }

        @Override
        public String toString() {
            return path().toString();
        }
    }
    
    public static abstract class TypedLabelZNode<T> extends ControlZNode implements Reference<T> {
        
        protected final T label;

        protected TypedLabelZNode(T label) {
            this(label, null);
        }
        
        protected TypedLabelZNode(T label, Object parent) {
            super(parent);
            this.label = label;
        }
        
        @Override
        public T get() {
            return label;
        }
        
        @Override
        public ZNodeLabel.Component label() {
            return ZNodeLabel.Component.of(toString());
        }

        @Override
        public String toString() {
            return Serializers.ToString.TO_STRING.apply(get());
        }
    }
    
    public static abstract class TypedValueZNode<T> extends ControlZNode implements Reference<T> {
        
        @SuppressWarnings("unchecked")
        public static <T, C extends TypedValueZNode<T>> C newInstance(Class<C> cls, T value, Object parent) {
            for (Constructor<?> c: cls.getDeclaredConstructors()) {
                Class<?>[] parameterTypes = c.getParameterTypes();
                if ((parameterTypes.length == 2) 
                        && parameterTypes[0].isAssignableFrom(value.getClass())
                        && parameterTypes[1].isAssignableFrom(parent.getClass())) {
                    try {
                        return (C) c.newInstance(value, parent);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
            }
            throw new AssertionError(Arrays.toString(cls.getDeclaredConstructors()));
        }
        
        @SuppressWarnings("unchecked")
        public static <T, C extends TypedValueZNode<T>> ListenableFuture<C> get(
                final Class<C> cls, 
                final Object parent, 
                final Materializer<?> materializer) {
            final ZNodeLabel.Path path = ZNodeLabel.Path.of(path(parent), path(cls).tail());
            return Futures.transform(
                    materializer.operator().getData(path).submit(),
                    new AsyncFunction<Operation.ProtocolResponse<Records.Response>, C>() {
                        @Override
                        public @Nullable
                        ListenableFuture<C> apply(Operation.ProtocolResponse<Response> input) throws KeeperException {
                            Operations.unlessError(input.getRecord());
                            T value = (T) materializer.get(path).get().get();
                            return Futures.immediateFuture(newInstance(cls, value, parent));
                        }
                    });
        }
        
        public static <T, C extends TypedValueZNode<T>> ListenableFuture<C> create(
                final Class<C> cls, 
                final T value, 
                final Object parent, 
                final Materializer<?> materializer) {
            final C instance = newInstance(cls, value, parent);
            return Futures.transform(
                    materializer.operator().create(instance.path(), instance.get()).submit(),
                    new AsyncFunction<Operation.ProtocolResponse<Records.Response>, C>() {
                        @Override
                        public @Nullable
                        ListenableFuture<C> apply(Operation.ProtocolResponse<Response> input) throws KeeperException {
                            Optional<Operation.Error> error = Operations.maybeError(input.getRecord(), KeeperException.Code.NODEEXISTS, input.toString());
                            if (error.isPresent()) {
                                return get(cls, parent, materializer);
                            } else {
                                return Futures.immediateFuture(instance);
                            }
                        }
                    });
        }
        
        protected final T value;

        protected TypedValueZNode(T value) {
            this(value, null);
        }
        
        protected TypedValueZNode(T value, Object parent) {
            super(parent);
            this.value = value;
        }
        
        @Override
        public T get() {
            return value;
        }

        @Override
        public String toString() {
            return get().toString();
        }
    }

    public static class FetchUntil<U extends Operation.ProtocolResponse<Records.Response>> extends PromiseTask<Materializer<U>, Void> implements FutureCallback<Void> {

        public static <T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<Records.Response>> FetchUntil<V> newInstance(ZNodeLabel.Path root, Predicate<Materializer<?>> predicate, Materializer<V> materializer) throws InterruptedException, ExecutionException {
            Promise<Void> delegate = newPromise();
            return new FetchUntil<V>(root, predicate, materializer, delegate);
        }
        
        protected class Updater implements Runnable {
            protected final ListenableFuture<Void> future;
            
            public Updater(ZNodeLabel.Path root) {
                this.future = TreeFetcher.<U,Void>builder().setClient(task()).setData(true).setWatch(true).build().apply(root);
                Futures.addCallback(future, FetchUntil.this, MoreExecutors.sameThreadExecutor());
                FetchUntil.this.addListener(this, MoreExecutors.sameThreadExecutor());
            }

            @Override
            public void run() {
                future.cancel(true);
            }
        }
        
        protected final ZNodeLabel.Path root;
        protected final Predicate<Materializer<?>> predicate;
        
        protected FetchUntil(
                ZNodeLabel.Path root, 
                Predicate<Materializer<?>> predicate, 
                Materializer<U> task, 
                Promise<Void> delegate) throws InterruptedException, ExecutionException {
            super(task, delegate);
            this.root = root;
            this.predicate = predicate;
            
            task().register(this);
            task().operator().sync(root).submit();
            new Updater(root);
        }

        @Subscribe
        public void handleReply(Operation.ProtocolResponse<?> message) {
            if (OpCodeXid.NOTIFICATION.getXid() == message.getXid()) {
                @SuppressWarnings("unchecked")
                WatchEvent event = WatchEvent.of((Operation.ProtocolResponse<IWatcherEvent>) message);
                if (root.prefixOf(event.getPath())) {
                    new Updater(event.getPath());
                }
            }
        }

        @Override
        public void onSuccess(Void result) {
            boolean done = predicate.apply(task());

            if (done) {
                set(null);
            } else {
                try {
                    // force watches
                    task().operator().sync(root).submit().get();
                } catch (Exception e) {
                    setException(e);
                }
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
                    task().unregister(this);
                } catch (IllegalArgumentException e) {}
            }
            return isSet;
        }
        
        @Override
        public boolean set(Void result) {
            boolean isSet = super.set(result);
            if (isSet) {
                try {
                    task().unregister(this);
                } catch (IllegalArgumentException e) {}
            }
            return isSet;
        }
        
        @Override
        public boolean setException(Throwable t) {
            boolean isSet = super.setException(t);
            if (isSet) {
                try {
                    task().unregister(this);
                } catch (IllegalArgumentException e) {}
            }
            return isSet;
        }
    }
    
    public static class RegisterHashedTask<O extends Operation.ProtocolResponse<Records.Response>, T, V extends ControlZNode> extends PromiseTask<T,V> implements Runnable {

        public static <O extends Operation.ProtocolResponse<Records.Response>, T, V extends ControlZNode>
        RegisterHashedTask<O,T,V> of(
                T task,
                Hash.Hashed hashed,
                Function<Identifier, V> entityOf,
                Function<V, ZNodeLabel.Path> pathOfValue,
                AsyncFunction<V, ? extends TypedValueZNode<T>> valueOf,
                Materializer<O> materializer,
                Executor executor) {
            Promise<V> promise = SettableFuturePromise.create();
            return new RegisterHashedTask<O,T,V>(task, hashed, entityOf, pathOfValue, valueOf, materializer, executor, promise);
        }
        
        protected final Executor executor;
        protected final Materializer<O> materializer;
        protected final Function<Identifier, V> entityOf;
        protected final Function<V, ZNodeLabel.Path> pathOfValue;
        protected final AsyncFunction<V, ? extends TypedValueZNode<T>> valueOf;
        protected volatile Hash.Hashed hashed;
        protected volatile ListenableFuture<O> createFuture;
        protected volatile ListenableFuture<? extends TypedValueZNode<T>> valueFuture;
        
        public RegisterHashedTask(
                T task,
                Hash.Hashed hashed,
                Function<Identifier, V> entityOf,
                Function<V, ZNodeLabel.Path> pathOfValue,
                AsyncFunction<V, ? extends TypedValueZNode<T>> valueOf,
                Materializer<O> materializer,
                Executor executor,
                Promise<V> delegate) {
            super(task, delegate);
            this.materializer = checkNotNull(materializer);
            this.executor = checkNotNull(executor);
            this.hashed = checkNotNull(hashed);
            this.entityOf = checkNotNull(entityOf);
            this.pathOfValue = checkNotNull(pathOfValue);
            this.valueOf = checkNotNull(valueOf);
            this.createFuture = null;
            this.valueFuture = null;
        }
        
        @Override
        public synchronized void run() {
            try {
                doRun();
            } catch (Throwable t) {
                setException(t);
            }
        }
        
        protected void doRun() throws Exception {
            if (isDone()) {
                return;
            }
            if (createFuture == null) {
                Identifier id = hashed.asIdentifier();
                while (id.equals(Identifier.zero())) {
                    hashed = hashed.rehash();
                    id = hashed.asIdentifier();
                }
                V entity = entityOf.apply(id);
                createFuture = materializer.submit(
                        Operations.Requests.multi()
                            .add(materializer.operator().create(
                                    entity.path()).get())
                            .add(materializer.operator().create(
                                    pathOfValue.apply(entity), 
                                    task()).get())
                            .build());
                createFuture.addListener(this, executor);
                return;
            } else if (! createFuture.isDone()) {
                return;
            } else {
                IMultiResponse response = (IMultiResponse) Operations.unlessError(createFuture.get().getRecord());
                Operation.Error error = null;
                for (Records.MultiOpResponse e: response) {
                    if (e instanceof Operation.Error) {
                        error = (Operation.Error) e;
                        if (error.getError() != KeeperException.Code.NODEEXISTS) {
                            throw KeeperException.create(error.getError());
                        }
                        break;
                    }
                }
                if (error == null) {
                    // success!
                    set(entityOf.apply(hashed.asIdentifier()));
                    return;
                } else {
                    // now, check that the existing node is my address!
                }
            }
            if (isDone()) {
                return;
            }
            assert ((createFuture != null) && createFuture.isDone());
            if (valueFuture == null) {
                valueFuture = valueOf.apply(entityOf.apply(hashed.asIdentifier()));
                valueFuture.addListener(this, executor);
            } else if (! valueFuture.isDone()) {
                return;
            } else {
                try {
                    T value = valueFuture.get().get();
                    assert (value != null);
                    if (task().equals(value)) {
                        // success!
                        set(entityOf.apply(hashed.asIdentifier()));
                        return;
                    } else {
                        // collision! try again...
                        hashed = hashed.rehash();
                        valueFuture = null;
                        createFuture = null;
                    }
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KeeperException.NoNodeException) {
                        // hmm...try again?
                        valueFuture = null;
                        createFuture = null;
                    } else {
                        throw e;
                    }
                }
            }
        }
    }
}
