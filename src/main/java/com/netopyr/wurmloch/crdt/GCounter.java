package com.netopyr.wurmloch.crdt;

import javaslang.Function4;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

public class GCounter extends AbstractCrdt implements Crdt {

    private final String nodeId;

    private Map<String, Long> entries = HashMap.empty();

    public GCounter(String nodeId, String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        super(id, inCommands, outCommands);
        this.nodeId = Objects.requireNonNull(nodeId, "NodeId must not be null");
    }

    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return GCounter::new;
    }

    @Override
    protected void processCommand(CrdtCommand command) {
        if (UpdateCommand.class.equals(command.getClass())) {
            final UpdateCommand updateCommand = (UpdateCommand) command;
            entries = entries.merge(updateCommand.entries, Math::max);
        }
    }

    public long get() {
        return entries.values().sum().longValue();
    }

    public void increment() {
        increment(1L);
    }

    public void increment(long value) {
        entries = entries.put(nodeId, entries.get(nodeId).getOrElse(0L) + value);
        commands.onNext(new UpdateCommand(
                id,
                entries
        ));
    }

    static final class UpdateCommand extends CrdtCommand {

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
