package edu.uw.zookeeper.safari.frontend;

import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.Executor;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingPromise;
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
import edu.uw.zookeeper.safari.frontend.ClientPeerConnectionDispatchers.ClientPeerConnectionDispatcher;
import edu.uw.zookeeper.safari.peer.protocol.MessagePacket;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenResponse;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionRequest;
import edu.uw.zookeeper.safari.peer.protocol.ShardedRequestMessage;
import edu.uw.zookeeper.safari.peer.protocol.ShardedResponseMessage;

public class ClientPeerConnectionExecutor extends ExecutedActor<ClientPeerConnectionExecutor.RequestTask> implements TaskExecutor<ShardedRequestMessage<?>, ShardedResponseMessage<?>>, PeerConnectionListener, Eventful<NotificationListener<ShardedResponseMessage<IWatcherEvent>>> {

    public static ListenableFuture<ClientPeerConnectionExecutor> connect(
            final Session frontend,
            final MessageSessionOpenRequest request,
            final Executor executor,
            final ListenableFuture<ClientPeerConnectionDispatcher> dispatcher) {
        Promise<ClientPeerConnectionExecutor> promise = SettableFuturePromise.create();
        return new ConnectTask(frontend, request, executor, dispatcher, promise);
    }
    
    public static ClientPeerConnectionExecutor newInstance(
            Session frontend,
            Session backend,
            ClientPeerConnectionDispatcher dispatcher,
            Executor executor) {
        IConcurrentSet<NotificationListener<ShardedResponseMessage<IWatcherEvent>>> listeners = new StrongConcurrentSet<NotificationListener<ShardedResponseMessage<IWatcherEvent>>>();
        return new ClientPeerConnectionExecutor(
                frontend, backend, dispatcher, executor, listeners);
    }
    
    public static class ConnectTask extends ForwardingListenableFuture<ClientPeerConnectionExecutor> 
            implements Runnable {
        
        protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
    
        protected final Session frontend;
        protected final MessageSessionOpenRequest request;
        protected final Executor executor;
        protected final ListenableFuture<ClientPeerConnectionDispatcher> dispatcher;
        protected final Promise<ClientPeerConnectionExecutor> promise;
        protected ListenableFuture<MessageSessionOpenResponse> response;
        
        public ConnectTask(
                Session frontend,
                MessageSessionOpenRequest request,
                Executor executor,
                ListenableFuture<ClientPeerConnectionDispatcher> dispatcher,
                Promise<ClientPeerConnectionExecutor> promise) {
            this.frontend = frontend;
            this.request = request;
            this.executor = executor;
            this.dispatcher = dispatcher;
            this.promise = promise;
            this.response = null;
            
            this.dispatcher.addListener(this, SAME_THREAD_EXECUTOR);
        }
        
        @Override
        public synchronized void run() {
            if (! isDone()) {
                try {
                    if (dispatcher.isDone()) {
                        if (response == null) {
                            response = dispatcher.get().connect(request);
                            response.addListener(this, SAME_THREAD_EXECUTOR);
                        } else if (response.isDone()) {
                            Session backend = response.get().getValue().toSession();
                            promise.set(newInstance(frontend, backend, dispatcher.get(), executor));
                        }
                    }
                } catch (Exception e) {
                    promise.setException(e);
                }
            }
        }
    
        @Override
        protected ListenableFuture<ClientPeerConnectionExecutor> delegate() {
            return promise;
        }
    }

    protected final Logger logger;
    protected final Session frontend;
    protected final Session backend;
    protected final ClientPeerConnectionDispatcher dispatcher;
    protected final Executor executor;
    protected final Queue<RequestTask> mailbox;
    protected final IConcurrentSet<NotificationListener<ShardedResponseMessage<IWatcherEvent>>> listeners;
    protected final Writer writer;
    
    protected ClientPeerConnectionExecutor(
            Session frontend,
            Session backend,
            ClientPeerConnectionDispatcher dispatcher,
            Executor executor,
            IConcurrentSet<NotificationListener<ShardedResponseMessage<IWatcherEvent>>> listeners) {
        this.logger = LogManager.getLogger(getClass());
        this.frontend = frontend;
        this.backend = backend;
        this.dispatcher = dispatcher;
        this.executor = executor;
        this.listeners = listeners;
        this.mailbox = Queues.newConcurrentLinkedQueue();
        this.writer = new Writer();
        
        dispatcher.subscribe(this);
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
        Promise<ShardedResponseMessage<?>> promise = SettableFuturePromise.create();
        promise = LoggingPromise.create(logger, promise);
        RequestTask task = new RequestTask(request, promise);
        if (! send(task)) {
            task.setException(new ClosedChannelException());
        }
        return task;
    }

    @Override
    public void subscribe(NotificationListener<ShardedResponseMessage<IWatcherEvent>> listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(NotificationListener<ShardedResponseMessage<IWatcherEvent>> listener) {
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
            for (NotificationListener<ShardedResponseMessage<IWatcherEvent>> listener: listeners) {
                listener.handleNotification((ShardedResponseMessage<IWatcherEvent>) message);
            }
        } else {
            writer.send(message);
        }
    }

    @Override
    protected boolean apply(RequestTask input) throws Exception {
        writer.submit(input);
        return true;
    }

    @Override
    protected void doStop() {
        dispatcher.unsubscribe(this);
        
        writer.stop();
        Exception e = new ClosedChannelException();
        RequestTask next;
        while ((next = mailbox.poll()) != null) {
            next.setException(e);
        }
        
        Iterator<NotificationListener<ShardedResponseMessage<IWatcherEvent>>> itr = Iterators.consumingIterator(listeners.iterator());
        while (itr.hasNext()) {
            itr.next();
        }
    }

    @Override
    protected Queue<RequestTask> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    protected static class RequestTask 
            extends PromiseTask<ShardedRequestMessage<?>, ShardedResponseMessage<?>>
            implements Operation.RequestId {

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
        public Promise<ShardedResponseMessage<?>> delegate() {
            return delegate;
        }
        
        @Override
        public boolean set(ShardedResponseMessage<?> response) {
            assert (response.xid() == task.xid());
            return super.set(response);
        }
    }
    
    protected class Writer extends AbstractActor<ShardedResponseMessage<?>>
            implements TaskExecutor<RequestTask, ShardedResponseMessage<?>> {

        protected final Queue<ResponseTask> pending;
        
        public Writer() {
            this.pending = Queues.newConcurrentLinkedQueue();
        }
        
        @Override
        public synchronized ListenableFuture<ShardedResponseMessage<?>> submit(RequestTask request) {
            MessagePacket<MessageSessionRequest> message = 
                    MessagePacket.of(MessageSessionRequest.of(frontend.id(), request.task()));
            ResponseTask task = new ResponseTask(request.xid(), request.delegate());
            // task in queue before sending...
            pending.add(task);
            if (state() == State.TERMINATED) {
                task.setException(new ClosedChannelException());
            } else {
                ListenableFuture<MessagePacket<MessageSessionRequest>> write = dispatcher.connection().write(message);
                Futures.addCallback(write, task);
            }
            return task;
        }
        
        @Override
        protected boolean doSend(ShardedResponseMessage<?> message) {
            ResponseTask next = pending.peek();
            if (message.xid() == next.xid()) {
                pending.remove(next);
                next.set(message);
            } else {
                throw new AssertionError(String.valueOf(message));
            }
            return true;
        }

        @Override
        protected void doRun() throws Exception {
        }

        @Override
        protected void doStop() {
            Exception e = new ClosedChannelException();
            ResponseTask next;
            while ((next = pending.poll()) != null) {
                next.setException(e);
            }
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }
    
    protected static class ResponseTask extends ForwardingPromise<ShardedResponseMessage<?>>
        implements Operation.RequestId, FutureCallback<Object> {

        protected final int xid;
        protected final Promise<ShardedResponseMessage<?>> promise;
        
        public ResponseTask(int xid, Promise<ShardedResponseMessage<?>> promise) {
            this.xid = xid;
            this.promise = promise;
        }

        @Override
        public int xid() {
            return xid;
        }
        
        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }

        @Override
        protected Promise<ShardedResponseMessage<?>> delegate() {
            return promise;
        }
    }
}
