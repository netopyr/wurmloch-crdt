package com.netopyr.wurmloch.crdt;

import com.netopyr.wurmloch.vectorclock.VectorClock;
import io.reactivex.processors.ReplayProcessor;
import javaslang.collection.Array;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;
import java.util.function.BiFunction;

public class MVRegister<T> extends AbstractCrdt<MVRegister<T>, MVRegister.SetCommand<T>> {

    // fields
    private Array<Entry<T>> entries = Array.empty();


    // constructor
    public MVRegister(String nodeId, String crdtId) {
        super(nodeId, crdtId, ReplayProcessor.create());
    }

    @Override
    public BiFunction<String, String, MVRegister<T>> getFactory() {
        return MVRegister::new;
    }


    // crdt
    protected boolean processCommand(SetCommand<T> command) {
        final Entry<T> newEntry = command.getEntry();
        if (!entries.exists(entry -> entry.getClock().compareTo(newEntry.getClock()) > 0
                || entry.getClock().equals(newEntry.getClock()))) {
            final Array<Entry<T>> newEntries = entries
                    .filter(entry -> entry.getClock().compareTo(newEntry.getClock()) == 0)
                    .append(newEntry);
            doSet(newEntries);
            return true;
        }
        return false;
    }


    // core functionality
    public Array<T> get() {
        return entries.map(Entry::getValue);
    }

    public void set(T newValue) {
        if (entries.size() != 1 || !Objects.equals(entries.head().getValue(), newValue)) {
            final Entry<T> newEntry = new Entry<>(newValue, incVV());
            doSet(Array.of(newEntry));
            commands.onNext(new SetCommand<>(
                    crdtId,
                    newEntry
            ));
        }
    }


    // implementation
    private void doSet(Array<Entry<T>> newEntries) {
        entries = newEntries;
    }

    private VectorClock incVV() {
        final Array<VectorClock> clocks = entries.map(Entry::getClock);
        final VectorClock mergedClock = clocks.reduceOption(VectorClock::merge).getOrElse(new VectorClock());
        return mergedClock.increment(nodeId);
    }


    static final class Entry<T> {

        private final T value;
        private final VectorClock clock;

        private Entry(T value, VectorClock clock) {
            this.value = value;
            this.clock = Objects.requireNonNull(clock, "Clock must not be null");
        }

        T getValue() {
            return value;
        }

        VectorClock getClock() {
            return clock;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Entry<?> entry = (Entry<?>) o;

            return new EqualsBuilder()
                    .append(value, entry.value)
                    .append(clock, entry.clock)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(value)
                    .append(clock)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("value", value)
                    .append("clock", clock)
                    .toString();
        }
    }


    // commands
    public static final class SetCommand<T> extends CrdtCommand {

        private final Entry<T> entry;

        SetCommand(String crdtId, Entry<T> entry) {
            super(crdtId);
            this.entry = Objects.requireNonNull(entry, "Entry must not be null");
        }

        Entry<T> getEntry() {
            return entry;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            SetCommand<?> that = (SetCommand<?>) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(entry, that.entry)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(entry)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("entry", entry)
                    .toString();
        }
    }
}
