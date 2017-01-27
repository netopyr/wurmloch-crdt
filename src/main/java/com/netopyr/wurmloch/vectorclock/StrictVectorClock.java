package com.netopyr.wurmloch.vectorclock;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Set;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class StrictVectorClock implements Comparable<StrictVectorClock> {

    private final String key;
    private final Map<String, Long> entries;

    public StrictVectorClock(String key, Map<String, Long> entries) {
        this.key = key;
        this.entries = entries;
    }
    public StrictVectorClock(String key) {
        this(key, HashMap.empty());
    }

    public StrictVectorClock increment() {
        final long counter = entries.get(key).map(value -> value + 1L).getOrElse(1L);
        return new StrictVectorClock(key, entries.put(key, counter));
    }

    @Override
    public int compareTo(StrictVectorClock other) {
        final Set<Long> diffs = calculateDiffs(other);
        final boolean isGreater = diffs.find(diff -> diff > 0).isDefined();
        final boolean isLess = diffs.find(diff -> diff < 0).isDefined();

        return (isGreater && isLess) ? key.compareTo(other.key)
                : isLess ? -1
                : isGreater ? 1
                : key.compareTo(other.key);
    }

    public StrictVectorClock merge(StrictVectorClock other) {
        return new StrictVectorClock(key, entries.merge(other.entries, Math::max));
    }

    private Set<Long> calculateDiffs(StrictVectorClock other) {
        final Set<String> allKeys = entries.keySet().addAll(other.entries.keySet());
        return allKeys.map(key -> entries.get(key).getOrElse(0L) - other.entries.get(key).getOrElse(0L));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        StrictVectorClock that = (StrictVectorClock) o;

        return calculateDiffs(that).forAll(diff -> diff == 0);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(entries)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("key", key)
                .append("entries", entries)
                .toString();
    }
}
