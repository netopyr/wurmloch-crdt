package com.netopyr.wurmloch.crdt;

import com.netopyr.wurmloch.vectorclock.VectorClock;
import javaslang.Function4;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

public class LWWRegister<T> extends AbstractCrdt implements Crdt {

    private final String nodeId;

    private T value;
    private VectorClock clock = new VectorClock();

    public LWWRegister(String nodeId, String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        super(id, inCommands, outCommands);
        this.nodeId = Objects.requireNonNull(nodeId, "NodeId must not be null");
    }

    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return LWWRegister::new;
    }

    public T get() {
        return value;
    }

    public void set(T newValue) {
        if (! Objects.equals(value, newValue)) {
            doSet(newValue);
            commands.onNext(new SetCommand<>(
                    id,
                    nodeId,
                    value,
                    clock
            ));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (SetCommand.class.equals(clazz)) {
            final SetCommand<T> setCommand = (SetCommand<T>) command;
            final int clockComparison = clock.compareTo(setCommand.getClock());
            if (clockComparison < 0 || (clockComparison == 0 & this.nodeId.compareTo(setCommand.getNodeId()) > 0)) {
                clock = clock.merge(setCommand.getClock());
                doSet(setCommand.getValue());
            }
        }
    }

    private void doSet(T value) {
        this.value = value;
        clock = clock.increment(nodeId);
    }

    static final class SetCommand<T> extends CrdtCommand {

        private final String nodeId;
        private final T value;
        private final VectorClock clock;

        SetCommand(String crdtId, String nodeId, T value, VectorClock clock) {
            super(crdtId);
            this.nodeId = Objects.requireNonNull(nodeId, "NodeId must not be null");
            this.value = value;
            this.clock = Objects.requireNonNull(clock, "Clock must not be null");
        }

        String getNodeId() {
            return nodeId;
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

            SetCommand<?> that = (SetCommand<?>) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(nodeId, that.nodeId)
                    .append(value, that.value)
                    .append(clock, that.clock)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(nodeId)
                    .append(value)
                    .append(clock)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("nodeId", nodeId)
                    .append("value", value)
                    .append("clock", clock)
                    .toString();
        }
    }
}
