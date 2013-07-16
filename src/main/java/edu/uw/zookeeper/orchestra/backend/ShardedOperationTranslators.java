package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.Identifier;
import edu.uw.zookeeper.util.Pair;

public class ShardedOperationTranslators {

    protected final Function<Identifier, Pair<ZNodeLabel.Path, ZNodeLabel.Path>> prefix;
    protected final ConcurrentMap<Identifier, OperationPrefixTranslator> translators;
    
    public ShardedOperationTranslators(
            Function<Identifier, Pair<ZNodeLabel.Path, ZNodeLabel.Path>> prefix) {
        this.prefix = prefix;
        this.translators = Maps.newConcurrentMap();
    }

    public OperationPrefixTranslator get(Identifier id) {
        OperationPrefixTranslator translator = translators.get(id);
        if (translator == null) {
            Pair<ZNodeLabel.Path, ZNodeLabel.Path> prefixes = prefix.apply(id);
            translators.putIfAbsent(id, OperationPrefixTranslator.of(prefixes.first(), prefixes.second()));
            translator = translators.get(id);
        }
        return translator;
    }
    
    public OperationPrefixTranslator remove(Identifier id) {
        return translators.remove(id);
    }
}
