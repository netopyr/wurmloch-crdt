package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.BehaviorProcessor;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.control.Option;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public class GCounter extends AbstractCrdt<GCounter, GCounter.UpdateCommand> {

    // fields
    private Map<String, Long> entries = HashMap.empty();


    // constructor
    public GCounter(String nodeId, String crdtId) {
        super(nodeId, crdtId, BehaviorProcessor.create());
    }


    // crdt
    @Override
    protected Option<UpdateCommand> processCommand(UpdateCommand command) {
        final Map<String, Long> oldEntries = entries;
        entries = entries.merge(command.entries, Math::max);
        return entries.equals(oldEntries)? Option.none() : Option.of(new UpdateCommand(crdtId, entries));
    }


    // core functionality
    public long get() {
        return entries.values().sum().longValue();
    }

    public void increment() {
        increment(1L);
    }

    public void increment(long value) {
        if (value < 1L) {
            throw new IllegalArgumentException("Value needs to be a positive number.");
        }
        entries = entries.put(nodeId, entries.get(nodeId).getOrElse(0L) + value);
        commands.onNext(new UpdateCommand(
                crdtId,
                entries
        ));
    }


    // commands
    public static final class UpdateCommand extends CrdtCommand {

        private final Map<String, Long> entries;

        private UpdateCommand(String crdtId, Map<String, Long> entries) {
            super(crdtId);
            this.entries = Objects.requireNonNull(entries, "Entries must not be null");
        }

        Map<String,Long> getEntries() {
            return entries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            UpdateCommand that = (UpdateCommand) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(entries, that.entries)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(entries)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("entries", entries)
                    .toString();
        }
    }
}
