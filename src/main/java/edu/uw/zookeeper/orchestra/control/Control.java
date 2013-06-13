package edu.uw.zookeeper.orchestra.control;


import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import edu.uw.zookeeper.client.Materializer;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.data.Acls;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.Schema;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNode;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.Schema.ZNodeSchema.Builder.ZNodeTraversal;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Reference;

public abstract class Control {
    
    public static JacksonModule.JacksonSerializer getByteCodec() {
        return JacksonModule.getSerializer();
    }
    
    public static Schema getSchema() {
        return SchemaHolder.getInstance().get();
    }
    
    public static ZNodeLabel.Path path(Object element) {
        return ControlZNode.path(element);
    }

    public static enum SchemaHolder implements Reference<Schema> {
        INSTANCE(Schema.of(Schema.ZNodeSchema.getDefault()));
        
        public static SchemaHolder getInstance() {
            return INSTANCE;
        }
        
        private final Schema schema;
        private final Map<Object, Schema.SchemaNode> byElement;
        
        private SchemaHolder(Schema schema) {
            this.schema = schema;
            
            Iterator<ZNodeTraversal.Element> itr = 
                    Schema.ZNodeSchema.Builder.traverse(Orchestra.class);
            ImmutableMap.Builder<Object, Schema.SchemaNode> byElement = ImmutableMap.builder();
            while (itr.hasNext()) {
                ZNodeTraversal.Element next = itr.next();
                Schema.ZNodeSchema nextSchema = next.getBuilder().build();
                ZNodeLabel.Path path = ZNodeLabel.Path.of(next.getPath(), ZNodeLabel.of(nextSchema.getLabel()));
                Schema.SchemaNode node = schema.add(path, nextSchema);
                byElement.put(next.getElement(), node);
            }
            this.byElement = byElement.build();
        }
    
        @Override
        public Schema get() {
            return schema;
        }
        
        public Schema.SchemaNode byElement(Object type) {
            return byElement.get(type);
        }
        
        @Override
        public String toString() {
            return get().toString();
        }
    }

    @ZNode(acl=Acls.Definition.ANYONE_ALL)
    public static abstract class ControlZNode {
        public static Schema.SchemaNode schemaNode(Object element) {
            return SchemaHolder.getInstance().byElement(element);
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

        public ZNodeLabel.Component label() {
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
        public static <T, C extends TypedValueZNode<T>> C get(Class<C> cls, Object parent, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(path(parent), path(cls).tail());
            Operation.SessionResult result = materializer.operator().getData(path).submit().get();
            Operations.unlessError(result.reply().reply(), result.toString());
            T value = (T) materializer.get(path).get().get();
            return newInstance(cls, value, parent);
        }
        
        public static <T, C extends TypedValueZNode<T>> C create(Class<C> cls, T value, Object parent, Materializer materializer) throws InterruptedException, ExecutionException, KeeperException {
            C instance = newInstance(cls, value, parent);
            Operation.SessionResult result = materializer.operator().create(instance.path(), instance.get()).submit().get();
            Operation.Reply reply = Operations.maybeError(result.reply().reply(), KeeperException.Code.NODEEXISTS, result.toString());
            if (reply instanceof Operation.Error) {
                return get(cls, parent, materializer);
            } else {
                return instance;
            }
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

    public static class FetchUntil extends PromiseTask<Materializer, Void> implements FutureCallback<ZNodeLabel.Path> {

        public static FetchUntil newInstance(ZNodeLabel.Path root, Predicate<Materializer> predicate, Materializer materializer, Executor executor) throws InterruptedException, ExecutionException {
            Promise<Void> delegate = newPromise();
            return new FetchUntil(root, predicate, materializer, delegate, executor);
        }
        
        public class Updater implements Runnable {
            protected final TreeFetcher fetcher;
            
            public Updater(ZNodeLabel.Path root) {
                this.fetcher = TreeFetcher.Builder.create().setExecutor(executor).setClient(task()).setData(true).setWatch(true).setRoot(root).build();
                Futures.addCallback(fetcher.future(), FetchUntil.this, executor);
                FetchUntil.this.addListener(this, executor);
            }

            @Override
            public void run() {
                fetcher.stop();
            }
        }
        
        protected final ZNodeLabel.Path root;
        protected final Executor executor;
        protected final Predicate<Materializer> predicate;
        
        protected FetchUntil(
                ZNodeLabel.Path root, Predicate<Materializer> predicate, Materializer task, Promise<Void> delegate, Executor executor) throws InterruptedException, ExecutionException {
            super(task, delegate);
            this.executor = executor;
            this.root = root;
            this.predicate = predicate;
            
            task().register(this);
            task().operator().sync(root).submit();
            new Updater(root);
        }

        @Subscribe
        public void handleReply(Operation.SessionReply message) {
            if (OpCodeXid.NOTIFICATION.xid() == message.xid()) {
                WatchEvent event = WatchEvent.of((IWatcherEvent) ((Operation.RecordHolder<?>)message.reply()).asRecord());
                if (root.prefixOf(event.path())) {
                    new Updater(event.path());
                }
            }
        }

        @Override
        public void onSuccess(ZNodeLabel.Path result) {
            boolean done = predicate.apply(task());

            if (done) {
                set(null);
            } else {
                try {
                    // force watches
                    task().operator().sync(result).submit().get();
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
}
