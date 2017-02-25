package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.BehaviorProcessor;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;
import java.util.function.BiFunction;

@SuppressWarnings("WeakerAccess")
public class PNCounter extends AbstractCrdt<PNCounter, PNCounter.UpdateCommand> {

    // fields
    private Map<String, Long> pEntries = HashMap.empty();
    private Map<String, Long> nEntries = HashMap.empty();


    // constructor
    public PNCounter(String nodeId, String crtdId) {
        super(nodeId, crtdId, BehaviorProcessor.create());
    }

    @Override
    public BiFunction<String, String, PNCounter> getFactory() {
        return PNCounter::new;
    }


    // crdt
    protected boolean processCommand(PNCounter.UpdateCommand command) {
        final Map<String, Long> oldPEntries = pEntries;
        final Map<String, Long> oldNEntries = nEntries;
        pEntries = pEntries.merge(command.pEntries, Math::max);
        nEntries = nEntries.merge(command.nEntries, Math::max);
        return ! (pEntries.equals(oldPEntries) && nEntries.equals(oldNEntries));
    }


    // core functionality
    public long get() {
        return pEntries.values().sum().longValue() - nEntries.values().sum().longValue();
    }

    public void increment() {
        increment(1L);
    }

    public void increment(long value) {
        if (value < 1L) {
            throw new IllegalArgumentException("Value needs to be a positive number.");
        }
        pEntries = pEntries.put(nodeId, pEntries.get(nodeId).getOrElse(0L) + value);
        commands.onNext(new UpdateCommand(
                crdtId,
                pEntries,
                nEntries
        ));
    }

    public void decrement() {
        decrement(1L);
    }

    public void decrement(long value) {
        if (value < 1L) {
            throw new IllegalArgumentException("Value needs to be a positive number.");
        }
        nEntries = nEntries.put(nodeId, nEntries.get(nodeId).getOrElse(0L) + value);
        commands.onNext(new UpdateCommand(
                crdtId,
                pEntries,
                nEntries
        ));
    }


    // commands
    public static final class UpdateCommand extends CrdtCommand {

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
