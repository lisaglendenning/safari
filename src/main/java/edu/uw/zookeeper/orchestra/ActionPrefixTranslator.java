package edu.uw.zookeeper.orchestra;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractPair;

public class ActionPrefixTranslator extends AbstractPair<ZNodeLabel.Path, ZNodeLabel.Path> implements Function<Operation.Action, Operation.Action> {

    public static ActionPrefixTranslator of(ZNodeLabel.Path fromPrefix, ZNodeLabel.Path toPrefix) {
        return new ActionPrefixTranslator(fromPrefix, toPrefix);
    }
    
    public ActionPrefixTranslator(ZNodeLabel.Path fromPrefix, ZNodeLabel.Path toPrefix) {
        super(fromPrefix, toPrefix);
    }
    
    public ZNodeLabel.Path getFromPrefix() {
        return first;
    }
    
    public ZNodeLabel.Path getToPrefix() {
        return second;
    }

    @Override
    public Operation.Action apply(Operation.Action input) {
        Operation.Action output = input;
        if (input instanceof Records.PathHolder) {
            Operations.PathBuilder<?> builder = (Operations.PathBuilder<?>) Operations.fromRecord(input);
            ZNodeLabel.Path path = builder.getPath();
            if (getFromPrefix().prefixOf(path)) {
                int prefixLen = getFromPrefix().length();
                ZNodeLabel.Path transformed;
                if (path.length() == prefixLen) {
                    transformed = getToPrefix();
                } else {
                    String remaining = path.toString().substring(prefixLen);
                    transformed = ZNodeLabel.Path.joined(getToPrefix().toString(), remaining);
                }
                builder.setPath(transformed);
                output = builder.build();
            }
        }
        return output;
    }
}
