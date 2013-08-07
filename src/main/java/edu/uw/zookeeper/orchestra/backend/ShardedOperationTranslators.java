package edu.uw.zookeeper.orchestra.backend;

import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.orchestra.common.Identifier;

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
            translators.putIfAbsent(id, newTranslator(id));
            translator = translators.get(id);
        }
        return translator;
    }
    
    public OperationPrefixTranslator remove(Identifier id) {
        return translators.remove(id);
    }
    
    protected OperationPrefixTranslator newTranslator(Identifier id) {
        Pair<ZNodeLabel.Path, ZNodeLabel.Path> prefixes = prefix.apply(id);
        return OperationPrefixTranslator.create(prefixes.first(), prefixes.second());
    }
}
