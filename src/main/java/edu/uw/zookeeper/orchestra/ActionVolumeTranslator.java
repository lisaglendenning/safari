package edu.uw.zookeeper.orchestra;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;

// TODO: what about volume changes?
public class ActionVolumeTranslator implements Function<Operation.Action, Operation.Action> {

    protected static final ZNodeLabel.Path PREFIX = ZNodeLabel.Path.of("/volumes");

    protected final VolumeLookupService lookup;
    protected final ConcurrentMap<Volume, ActionPrefixTranslator> translators;
    
    public ActionVolumeTranslator(VolumeLookupService lookup) {
        this.lookup = lookup;
        this.translators = new ConcurrentHashMap<Volume, ActionPrefixTranslator>();
    }
    
    @Override
    public Operation.Action apply(Operation.Action input) {
        Operation.Action output = input;
        if (input instanceof IMultiRequest) {
            IMultiRequest record = new IMultiRequest();
            for (Records.MultiOpRequest e: (IMultiRequest)input) {
                record.add((Records.MultiOpRequest) apply(e));
            }
            output = record;
        } else if (input instanceof IMultiResponse) {
            IMultiResponse record = new IMultiResponse();
            for (Records.MultiOpResponse e: (IMultiResponse)input) {
                record.add((Records.MultiOpResponse) apply(e));
            }
            output = record;
        } else if (input instanceof Records.PathHolder) {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathHolder) input).getPath());
            Volume volume = lookup.get(path).getVolume();
            ActionPrefixTranslator translator = getTranslator(volume);
            output = translator.apply(input);
        }
        return output;
    }

    protected ActionPrefixTranslator getTranslator(Volume volume) {
        ActionPrefixTranslator translator = translators.get(volume);
        if (translator == null) {
            ZNodeLabel.Path fromPrefix = volume.getDescriptor().getRoot();
            ZNodeLabel.Path toPrefix = ZNodeLabel.Path.of(PREFIX, ZNodeLabel.Component.of(volume.getId().toString()));
            translators.putIfAbsent(volume, ActionPrefixTranslator.of(fromPrefix, toPrefix));
            translator = translators.get(volume);
        }
        return translator;
    }
}
