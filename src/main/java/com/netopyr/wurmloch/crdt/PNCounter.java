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

public class PNCounter extends AbstractCrdt implements Crdt {

    private final String nodeId;

    private Map<String, Long> pEntries = HashMap.empty();
    private Map<String, Long> nEntries = HashMap.empty();

    public PNCounter(String nodeId, String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        super(id, inCommands, outCommands);
        this.nodeId = Objects.requireNonNull(nodeId, "NodeId must not be null");
    }

    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return PNCounter::new;
    }

    @Override
    protected void processCommand(CrdtCommand command) {
        if (UpdateCommand.class.equals(command.getClass())) {
            final UpdateCommand updateCommand = (UpdateCommand) command;
            pEntries = pEntries.merge(updateCommand.pEntries, Math::max);
            nEntries = nEntries.merge(updateCommand.nEntries, Math::max);
        }
    }

    public long get() {
        return pEntries.values().sum().longValue() - nEntries.values().sum().longValue();
    }

    public void increment() {
        increment(1L);
    }

    public void increment(long value) {
        pEntries = pEntries.put(nodeId, pEntries.get(nodeId).getOrElse(0L) + value);
        commands.onNext(new UpdateCommand(
                id,
                pEntries,
                nEntries
        ));
    }

    public void decrement() {
        decrement(1L);
    }

    public void decrement(long value) {
        nEntries = nEntries.put(nodeId, nEntries.get(nodeId).getOrElse(0L) + value);
        commands.onNext(new UpdateCommand(
                id,
                pEntries,
                nEntries
        ));
    }

    static final class UpdateCommand extends CrdtCommand {

        private final Map<String, Long> pEntries;
        private final Map<String, Long> nEntries;

        private UpdateCommand(String crdtId, Map<String, Long> pEntries, Map<String, Long> nEntries) {
            super(crdtId);
            this.pEntries = Objects.requireNonNull(pEntries, "PEntries must not be null");
            this.nEntries = Objects.requireNonNull(nEntries, "NEntries must not be null");
        }

        Map<String,Long> getPEntries() {
            return pEntries;
        }

        Map<String,Long> getNEntries() {
            return nEntries;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            UpdateCommand that = (UpdateCommand) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(pEntries, that.pEntries)
                    .append(nEntries, that.nEntries)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(pEntries)
                    .append(nEntries)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("pEntries", pEntries)
                    .append("nEntries", nEntries)
                    .toString();
        }
    }
}
