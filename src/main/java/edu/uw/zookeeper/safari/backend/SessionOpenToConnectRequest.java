package edu.uw.zookeeper.safari.backend;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.Materializer;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.safari.peer.protocol.MessageSessionOpenRequest;
import edu.uw.zookeeper.safari.storage.StorageSchema;
import edu.uw.zookeeper.safari.storage.StorageZNode;

public class SessionOpenToConnectRequest implements AsyncFunction<MessageSessionOpenRequest, ConnectMessage.Request> {

    public static SessionOpenToConnectRequest create(
            ZxidReference zxids,
            Materializer<StorageZNode<?>,?> materializer) {
        return new SessionOpenToConnectRequest(zxids, materializer);
    }
    
    private final Logger logger;
    private final ZxidReference zxids;
    private final Materializer<StorageZNode<?>,?> materializer;
    
    public SessionOpenToConnectRequest(
            ZxidReference zxids,
            Materializer<StorageZNode<?>,?> materializer) {
        this.logger = LogManager.getLogger(this);
        this.zxids = zxids;
        this.materializer = materializer;
    }
    
    @Override
    public ListenableFuture<ConnectMessage.Request> apply(MessageSessionOpenRequest request) {
        final ConnectMessage.Request message = request.getMessage();
        final ConnectMessage.Request value;
        if (message instanceof ConnectMessage.Request.NewRequest) {
            if ((message.getPasswd() != null) && (message.getPasswd().length > 0)) {
                SessionLookup lookup = SessionLookup.create(
                        request.getIdentifier(), 
                        materializer,
                        LoggingPromise.create(logger,
                                SettableFuturePromise.<StorageSchema.Safari.Sessions.Session.Data>create()));
                return Futures.transform(lookup, new SessionLookupCallback(message), SameThreadExecutor.getInstance());
            } else {
                value = ConnectMessage.Request.NewRequest.newInstance(
                        TimeValue.milliseconds(message.getTimeOut()), 
                            zxids.get());
            }
        } else {
            value = ConnectMessage.Request.RenewRequest.newInstance(
                    message.toSession(), zxids.get());
        }
        return Futures.immediateFuture(value);
    }
    
    public class SessionLookupCallback implements Function<StorageSchema.Safari.Sessions.Session.Data, ConnectMessage.Request> {

        private final ConnectMessage.Request request;
        
        public SessionLookupCallback(ConnectMessage.Request request) {
            this.request = request;
        }
        
        @Override
        public ConnectMessage.Request apply(StorageSchema.Safari.Sessions.Session.Data input) {
            return ConnectMessage.Request.RenewRequest.newInstance(
                    Session.create(
                            input.getSessionId().longValue(), 
                            Session.Parameters.create(
                                    request.toSession().parameters().timeOut(),
                                    input.getPassword())), 
                    zxids.get());
        }
    }
}