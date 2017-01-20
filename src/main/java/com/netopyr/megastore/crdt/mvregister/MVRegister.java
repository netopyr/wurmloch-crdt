package com.netopyr.megastore.crdt.mvregister;

import com.netopyr.megastore.crdt.AbstractCrdt;
import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.vectorclock.VectorClock;
import javaslang.Function4;
import javaslang.collection.Array;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

public class MVRegister<T> extends AbstractCrdt implements Crdt {

    private final String nodeId;

    private Array<Entry<T>> entries = Array.empty();

    public MVRegister(String nodeId, String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        super(id, inCommands, outCommands);
        this.nodeId = Objects.requireNonNull(nodeId, "NodeId must not be null");
    }

    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return MVRegister::new;
    }

    public Array<T> get() {
        return entries.map(Entry::getValue);
    }

    public void set(T newValue) {
        if (entries.size() != 1 || ! Objects.equals(entries.head().getValue(), newValue)) {
            final Entry<T> newEntry = new Entry<>(newValue, incVV());
            doSet(Array.of(newEntry));
            commands.onNext(new SetMVCommand<>(
                    id,
                    newEntry
            ));
        }
    }

    private void doSet(Array<Entry<T>> newEntries) {
        entries = newEntries;
    }

    private VectorClock incVV() {
        final Array<VectorClock> clocks = entries.map(Entry::getClock);
        final VectorClock mergedClock = clocks.reduceOption(VectorClock::merge).getOrElse(new VectorClock());
        return mergedClock.increment(nodeId);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void processCommand(CrdtCommand command) {
        if (command instanceof SetMVCommand) {
            final Entry<T> newEntry = ((SetMVCommand<T>)command).getEntry();
            if (entries.exists(entry -> entry.getClock().compareTo(newEntry.getClock()) > 0)) {
                return;
            }
            final Array<Entry<T>> newEntries = entries
                    .filter(entry -> entry.getClock().compareTo(newEntry.getClock()) == 0)
                    .filter(entry -> ! entry.getClock().isIdentical(newEntry.getClock()))
                    .append(newEntry);
            doSet(newEntries);
        }
    }


    static final class Entry<T> {

        private final T value;
        private final VectorClock clock;

        private Entry(T value, VectorClock clock) {
            this.value = value;
            this.clock = clock;
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

}
