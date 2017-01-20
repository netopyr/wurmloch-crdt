package com.netopyr.wurmloch.vectorclock;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.collection.Set;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class VectorClock implements Comparable<VectorClock> {

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

    private Set<Long> calculateDiffs(VectorClock other) {
        final Set<String> allKeys = entries.keySet().addAll(other.entries.keySet());
        return allKeys.map(key -> entries.get(key).getOrElse(0L) - other.entries.get(key).getOrElse(0L));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        VectorClock that = (VectorClock) o;

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
                .append("entries", entries)
                .toString();
    }
}
