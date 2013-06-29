package edu.uw.zookeeper.orchestra;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.client.AssignXidProcessor;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyMessage;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.server.AssignZxidProcessor;
import edu.uw.zookeeper.server.ExpiringSessionManager;
import edu.uw.zookeeper.server.ServerExecutor;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Processors;
import edu.uw.zookeeper.util.Publisher;

public class ProxyServerExecutor extends ServerExecutor {

    public static ProxyServerExecutor newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            Factory<ClientProtocolExecutor> clientFactory) {
        AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        AssignXidProcessor xids = AssignXidProcessor.newInstance();
        return newInstance(
                executor,
                publisherFactory,
                sessions, 
                zxids,
                xids,
                clientFactory);
    }
    
    public static ProxyServerExecutor newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Factory<ClientProtocolExecutor> clientFactory) {
        return new ProxyServerExecutor(
                executor,
                publisherFactory,
                sessions,
                zxids, 
                xids, 
                Processors.<Operation.Request>identity(),
                ProxyReplyProcessor.newInstance(zxids),
                clientFactory);
    }

    public static class ProxyReplyProcessor implements Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> {

        public static ProxyReplyProcessor newInstance(
                AssignZxidProcessor zxids) {
            return new ProxyReplyProcessor(zxids);
        }
        
        protected final AssignZxidProcessor zxids;
        
        protected ProxyReplyProcessor(
                AssignZxidProcessor zxids) {
            this.zxids = zxids;
        }
        
        @Override
        public Operation.SessionReply apply(Pair<Optional<Operation.SessionRequest>, Operation.SessionReply> input) throws Exception {
            Optional<Operation.SessionRequest> request = input.first();
            Operation.SessionReply reply = input.second();
            
            int xid;
            if (request.isPresent()){
                xid = request.get().xid();
            } else {
                xid = reply.xid();
            }
            
            Operation.Response payload = reply.reply();
            Long zxid = zxids.apply(payload);
            return SessionReplyMessage.newInstance(xid, zxid, payload);
        }
    }

    protected final Factory<ClientProtocolExecutor> clientFactory;
    protected final AssignXidProcessor xids;
    protected final Processor<Operation.Request, Operation.Request> requestProcessor;
    protected final Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> replyProcessor;
    
    protected ProxyServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids,
            AssignXidProcessor xids,
            Processor<Operation.Request, Operation.Request> requestProcessor,
            Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> replyProcessor,
            Factory<ClientProtocolExecutor> clientFactory) {
        super(executor, publisherFactory, sessions, zxids);
        this.xids = xids;
        this.clientFactory = clientFactory;
        this.requestProcessor = requestProcessor;
        this.replyProcessor = replyProcessor;
    }
    
    public AssignXidProcessor xids() {
        return xids;
    }
    
    public Processor<Operation.Request, Operation.Request> asRequestProcessor() {
        return requestProcessor;
    }
    
    public Processor<Pair<Optional<Operation.SessionRequest>, Operation.SessionReply>, Operation.SessionReply> asReplyProcessor() {
        return replyProcessor;
    }
    
    @Override
    protected PublishingSessionRequestExecutor newSessionRequestExecutor(Long sessionId) {
        ClientProtocolExecutor client = clientFactory.get();
        return ProxyRequestExecutor.newInstance(
                publisherFactory.get(), this, sessionId, client);
    }
}
