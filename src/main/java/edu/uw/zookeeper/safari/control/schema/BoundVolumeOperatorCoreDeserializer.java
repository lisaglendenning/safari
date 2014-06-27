package edu.uw.zookeeper.safari.control.schema;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.jackson.ListCoreDeserializer;
import edu.uw.zookeeper.safari.volume.AssignParameters;
import edu.uw.zookeeper.safari.volume.MergeParameters;
import edu.uw.zookeeper.safari.volume.BoundVolumeOperator;
import edu.uw.zookeeper.safari.volume.VolumeOperator;
import edu.uw.zookeeper.safari.volume.VolumeOperatorParameters;
import edu.uw.zookeeper.safari.volume.SplitParameters;

public class BoundVolumeOperatorCoreDeserializer extends ListCoreDeserializer<BoundVolumeOperator<?>> {

    public static BoundVolumeOperatorCoreDeserializer create() {
        return new BoundVolumeOperatorCoreDeserializer(
                getTypes());
    }
    
    public static ImmutableMap<VolumeOperator,Class<? extends VolumeOperatorParameters>> getTypes() {
        return ImmutableMap.<VolumeOperator, Class<? extends VolumeOperatorParameters>>of(
                VolumeOperator.TRANSFER, AssignParameters.class,
                VolumeOperator.SPLIT, SplitParameters.class,
                VolumeOperator.MERGE, MergeParameters.class);
    }

    protected final ImmutableMap<VolumeOperator, Class<? extends VolumeOperatorParameters>> types;
    
    protected BoundVolumeOperatorCoreDeserializer(ImmutableMap<VolumeOperator, Class<? extends VolumeOperatorParameters>> types) {
        this.types = types;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends BoundVolumeOperator> handledType() {
        return BoundVolumeOperator.class;
    }

    @Override
    protected BoundVolumeOperator<?> deserializeValue(JsonParser json)
            throws IOException, JsonProcessingException {
        JsonToken token = json.getCurrentToken();
        if (token == null) {
            token = json.nextToken();
            if (token == null) {
                return null;
            }
        }
        if (token != JsonToken.VALUE_STRING) {
            throw new JsonParseException(String.valueOf(json.getCurrentToken()), json.getCurrentLocation());
        }
        VolumeOperator operator = VolumeOperator.valueOf(json.getText());
        json.nextToken();
        VolumeOperatorParameters body = json.readValueAs(types.get(operator));
        BoundVolumeOperator<?> value = BoundVolumeOperator.valueOf(operator, body);
        return value;
    }
}
