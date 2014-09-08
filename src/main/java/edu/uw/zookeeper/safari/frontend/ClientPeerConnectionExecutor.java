package edu.uw.zookeeper.safari.frontend;

import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.NotificationListener;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.safari.Identifier;
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedServerResponseMessage;

public class ClientPeerConnectionExecutor extends AbstractActor<ClientPeerConnectionExecutor.RequestTask> implements TaskExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>>, SessionPeerListener, Eventful<NotificationListener<ShardedServerResponseMessage<IWatcherEvent>>> {

    public static ListenableFuture<ClientPeerConnectionExecutor> connect(
            final Session frontend,
            final MessageSessionOpenRequest request,
            final ListenableFuture<ClientPeerConnectionDispatcher> dispatcher) {
        Promise<ClientPeerConnectionExecutor> promise = SettableFuturePromise.create();
        return new ConnectTask(frontend, request, dispatcher, promise);
    }
    
    public static ClientPeerConnectionExecutor newInstance(
            Session frontend,
            Session backend,
            ClientPeerConnectionDispatcher dispatcher) {
        IConcurrentSet<NotificationListener<ShardedServerResponseMessage<IWatcherEvent>>> listeners = new StrongConcurrentSet<NotificationListener<ShardedServerResponseMessage<IWatcherEvent>>>();
        return new ClientPeerConnectionExecutor(
                frontend, backend, dispatcher, listeners,
                LogManager.getLogger(ClientPeerConnectionExecutor.class));
    }
    
    public static class ConnectTask extends ForwardingListenableFuture<ClientPeerConnectionExecutor> 
            implements Runnable {
        
        protected final Session frontend;
        protected final MessageSessionOpenRequest request;
        protected final ListenableFuture<ClientPeerConnectionDispatcher> dispatcher;
        protected final Promise<ClientPeerConnectionExecutor> promise;
        protected Optional<ListenableFuture<MessageSessionOpenResponse>> response;
        
        public ConnectTask(
                Session frontend,
                MessageSessionOpenRequest request,
                ListenableFuture<ClientPeerConnectionDispatcher> dispatcher,
                Promise<ClientPeerConnectionExecutor> promise) {
            this.frontend = frontend;
            this.request = request;
            this.dispatcher = dispatcher;
            this.promise = promise;
            this.response = Optional.absent();
            
            this.dispatcher.addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public synchronized void run() {
            if (! isDone()) {
                try {
                    if (dispatcher.isDone()) {
                        if (!response.isPresent()) {
                            response = Optional.of(dispatcher.get().connect(request));
                            response.get().addListener(this, MoreExecutors.directExecutor());
                        } else if (response.get().isDone()) {
                            Session backend = response.get().get().getMessage().toSession();
                            promise.set(newInstance(frontend, backend, dispatcher.get()));
                        }
                    }
                } catch (Exception e) {
                    if (e instanceof ExecutionException) {
                        promise.setException(e.getCause());
                    } else {
                        promise.setException(e);
                    }
                }
            }
        }
    
        @Override
        protected ListenableFuture<ClientPeerConnectionExecutor> delegate() {
            return promise;
        }
    }

    protected final Session frontend;
    protected final Session backend;
    protected final ClientPeerConnectionDispatcher dispatcher;
    protected final IConcurrentSet<NotificationListener<ShardedServerResponseMessage<IWatcherEvent>>> listeners;
    protected final ConcurrentMap<Identifier, Writer> writers;
    
    protected ClientPeerConnectionExecutor(
            Session frontend,
            Session backend,
            ClientPeerConnectionDispatcher dispatcher,
            IConcurrentSet<NotificationListener<ShardedServerResponseMessage<IWatcherEvent>>> listeners,
            Logger logger) {
        super(logger);
        this.frontend = frontend;
        this.backend = backend;
        this.dispatcher = dispatcher;
        this.listeners = listeners;
        this.writers = new MapMaker().makeMap();
        
        dispatcher.subscribe(this);
    }
    
    public Writer writer(Identifier volume) {
        return writers.get(volume);
    }
    
    public ClientPeerConnectionDispatcher dispatcher() {
        return dispatcher;
    }
    
    public Session backend() {
        return backend;
    }
    
    @Override
    public Session session() {
        return frontend;
    }

    @Override
    public ListenableFuture<ShardedResponseMessage<?>> submit(
            ShardedRequestMessage<?> request) {
        RequestTask task = new RequestTask(request,
                SettableFuturePromise.<ShardedResponseMessage<?>>create());
        LoggingFutureListener.listen(logger, task);
        if (! send(task)) {
            task.cancel(true);
            throw new RejectedExecutionException();
        }
        return task;
    }

    @Override
    public void subscribe(NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> listener) {
        return listeners.remove(listener);
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> state) {
        if (state.to() == Connection.State.CONNECTION_CLOSED) {
            stop();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleConnectionRead(ShardedResponseMessage<?> message) {
        int xid = message.xid();
        if (xid == OpCodeXid.NOTIFICATION.xid()) {
            for (NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> listener: listeners) {
                listener.handleNotification((ShardedServerResponseMessage<IWatcherEvent>) message);
            }
        } else {
            Writer writer = writers.get(message.getShard().getValue());
            if (writer != null) {
                writer.send(message);
            } else {
                assert (state() == State.TERMINATED);
            }
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("frontend", Session.toString(frontend.id())).add("backend", Session.toString(backend.id())).toString();
    }
    
    @Override
    protected boolean doSend(RequestTask message) {
        final Identifier id = message.task().getShard().getValue();
        Writer writer = writers.get(id);
        if (writer == null) {
            writer = new Writer();
            Writer existing = writers.putIfAbsent(id, writer);
            if (existing != null) {
                writer = existing;
            }
        }
        try {
            writer.submit(message);
        } catch (RejectedExecutionException e) {
            return false;
        }
        return true;
    }

    @Override
    protected void doRun() throws Exception {
    }

    @Override
    protected void doStop() {
        dispatcher.unsubscribe(this);
        
        for (Writer writer: Iterables.consumingIterable(writers.values())) {
            writer.stop();
        }
        
        for (@SuppressWarnings("unused") NotificationListener<ShardedServerResponseMessage<IWatcherEvent>> listener:  Iterables.consumingIterable(listeners)) {
        }
    }

    protected static class RequestTask 
            extends PromiseTask<ShardedRequestMessage<?>, ShardedResponseMessage<?>>
            implements Operation.RequestId, FutureCallback<Object> {

        public RequestTask(
                ShardedRequestMessage<?> task,
                Promise<ShardedResponseMessage<?>> promise) {
            super(task, promise);
        }
        
        @Override
        public int xid() {
            return task.xid();
        }
        
        @Override
        public boolean set(ShardedResponseMessage<?> response) {
            assert (response.xid() == task.xid());
            return super.set(response);
        }
        
        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
    
    // TODO: garbage collect unused
    public final class Writer extends AbstractActor<ShardedResponseMessage<?>>
            implements TaskExecutor<RequestTask, ShardedResponseMessage<?>> {

        protected final Queue<RequestTask> pending;
        
        public Writer() {
            super(ClientPeerConnectionExecutor.this.logger);
            this.pending = Queues.<RequestTask>newConcurrentLinkedQueue();
        }
        
        public Queue<RequestTask> pending() {
            return pending;
        }
        
        /**
         * Don't call concurrently
         */
        @Override
        public RequestTask submit(final RequestTask task) {
            MessagePacket<MessageSessionRequest> message = 
                    MessagePacket.of(MessageSessionRequest.of(frontend.id(), task.task()));
            // task in queue before writing
            if (pending.offer(task)) {
                if (state() != State.TERMINATED) {
                    ListenableFuture<MessagePacket<MessageSessionRequest>> write = dispatcher.connection().write(message);
                    Futures.addCallback(write, task);
                    task.addListener(this, MoreExecutors.directExecutor());
                } else {
                    task.cancel(true);
                    pending.remove(task);
                    throw new RejectedExecutionException();
                }
            } else {
                task.cancel(true);
                throw new RejectedExecutionException();
            }
            return task;
        }
        
        /**
         * Don't call concurrently
         */
        @Override
        protected boolean doSend(ShardedResponseMessage<?> message) {
            RequestTask next = pending.peek();
            if ((next != null) && (next.xid() == message.xid())) {
                next.set(message);
                return true;
            } else {
                assert (state() == State.TERMINATED);
                return false;
            }
        }

        @Override
        protected boolean schedule() {
            RequestTask next = pending.peek();
            if ((next != null) && next.isDone() && state.compareAndSet(State.WAITING, State.SCHEDULED)) {
                doSchedule();
                return true;
            } else {
                return false;
            }
        }
        
        @Override
        protected void doRun() throws Exception {
            RequestTask next = pending.peek();
            while ((next != null) && next.isDone()) {
                pending.remove(next);
                next = pending.peek();
            }  
        }

        @Override
        protected void runExit() {
            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                schedule();
            }
        }
        
        @Override
        protected void doStop() {
            RequestTask next;
            while ((next = pending.poll()) != null) {
                next.cancel(true);
            }
        }
    }
}
