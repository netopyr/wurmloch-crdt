package com.netopyr.wurmloch.crdt;

import com.netopyr.wurmloch.vectorclock.StrictVectorClock;
import io.reactivex.processors.BehaviorProcessor;
import javaslang.control.Option;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public class LWWRegister<T> extends AbstractCrdt<LWWRegister<T>, LWWRegister.SetCommand<T>> {

    // fields
    private T value;
    private StrictVectorClock clock;


    // constructor
    public LWWRegister(String nodeId, String crdtId) {
        super(nodeId, crdtId, BehaviorProcessor.create());
        this.clock = new StrictVectorClock(nodeId);
    }


    // crdt
    protected Option<SetCommand<T>> processCommand(SetCommand<T> command) {
        if (clock.compareTo(command.getClock()) < 0) {
            clock = clock.merge(command.getClock());
            doSet(command.getValue());
            return Option.of(command);
        }
        return Option.none();
    }


    // core functionality
    public T get() {
        return value;
    }

    public void set(T newValue) {
        if (! Objects.equals(value, newValue)) {
            doSet(newValue);
            commands.onNext(new SetCommand<>(
                    crdtId,
                    value,
                    clock
            ));
        }
    }


    // implementation
    private void doSet(T value) {
        this.value = value;
        clock = clock.increment();
    }


    // commands
    public static final class SetCommand<T> extends CrdtCommand {

        private final T value;
        private final StrictVectorClock clock;

        SetCommand(String crdtId, T value, StrictVectorClock clock) {
            super(crdtId);
            this.value = value;
            this.clock = Objects.requireNonNull(clock, "Clock must not be null");
        }

        T getValue() {
            return value;
        }

        StrictVectorClock getClock() {
            return clock;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            SetCommand<?> that = (SetCommand<?>) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(value, that.value)
                    .append(clock, that.clock)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(value)
                    .append(clock)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("value", value)
                    .append("clock", clock)
                    .toString();
        }
    }
}
