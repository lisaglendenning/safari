package edu.uw.zookeeper.safari.frontend;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.AssignZxidProcessor;
import edu.uw.zookeeper.protocol.server.ZxidGenerator;
import edu.uw.zookeeper.server.SessionManager;

public class ResponseProcessor implements Processors.UncheckedProcessor<Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>>, Message.ServerResponse<?>> {

    public static ResponseProcessor create(
            SessionManager sessions,
            ZxidGenerator zxids) {
        return new ResponseProcessor(sessions, AssignZxidProcessor.newInstance(zxids));
    }
    
    protected final SessionManager sessions;
    protected final AssignZxidProcessor zxids;
    
    protected ResponseProcessor(
            SessionManager sessions,
            AssignZxidProcessor zxids) {
        this.sessions = sessions;
        this.zxids = zxids;
    }
    
    @Override
    public Message.ServerResponse<?> apply(Pair<Long, Pair<Optional<Operation.ProtocolRequest<?>>, Records.Response>> input) {
        Optional<Operation.ProtocolRequest<?>> request = input.second().first();
        Records.Response response = input.second().second();
        int xid;
        if (response instanceof Operation.RequestId) {
            xid = ((Operation.RequestId) response).xid();
        } else {
            xid = request.get().xid();
        }
        OpCode opcode;
        if (OpCodeXid.has(xid)) {
            opcode = OpCodeXid.of(xid).opcode();
        } else {
            opcode = request.get().record().opcode();
        }
        long zxid = zxids.apply(opcode);
        if (opcode == OpCode.CLOSE_SESSION) {
            sessions.remove(input.first());
        }
        return ProtocolResponseMessage.of(xid, zxid, response);
    }
}