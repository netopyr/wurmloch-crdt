package com.netopyr.megastore.vectorclock;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Set;

public class VectorClock implements Comparable<VectorClock> {

    private final Map<String, Long> entries;

    public VectorClock(Map<String, Long> entries) {
        this.entries = entries;
    }
    public VectorClock() {
        this(HashMap.empty());
    }

    public VectorClock increment(String key) {
        final long counter = entries.get(key).map(value -> value + 1L).getOrElse(1L);
        return new VectorClock(entries.put(key, counter));
    }

    @Override
    public int compareTo(VectorClock other) {
        final Set<Long> diffs = calculateDiffs(other);
        final boolean isGreater = diffs.find(diff -> diff > 0).isDefined();
        final boolean isLess = diffs.find(diff -> diff < 0).isDefined();

        return (isGreater && isLess) ? 0
                : isLess ? -1
                : isGreater ? 1
                : 0;
    }

    public VectorClock merge(VectorClock other) {
        return new VectorClock(entries.merge(other.entries, Math::max));
    }

    public boolean isIdentical(VectorClock other) {
        final Set<Long> diffs = calculateDiffs(other);
        return diffs.forAll(diff -> diff == 0L);
    }

    private Set<Long> calculateDiffs(VectorClock other) {
        final Set<String> allKeys = entries.keySet().addAll(other.entries.keySet());
        return allKeys.map(key -> entries.get(key).getOrElse(0L) - other.entries.get(key).getOrElse(0L));
    }

}
