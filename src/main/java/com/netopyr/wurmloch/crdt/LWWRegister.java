package com.netopyr.wurmloch.crdt;

import com.netopyr.wurmloch.vectorclock.StrictVectorClock;
import io.reactivex.processors.PublishProcessor;
import javaslang.Function4;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

public class LWWRegister<T> implements Crdt {

    private final String id;
    private final Processor<CrdtCommand, CrdtCommand> commands = PublishProcessor.create();

    private T value;
    private StrictVectorClock clock;

    public LWWRegister(String nodeId, String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        this.id = Objects.requireNonNull(id, "Id must not be null");
        Objects.requireNonNull(nodeId, "NodeId must not be null");
        this.clock = new StrictVectorClock(nodeId);

        inCommands = Objects.requireNonNull(inCommands, "InCommands must not be null");
        outCommands = Objects.requireNonNull(outCommands, "OutCommands must not be null");
        inCommands.subscribe(new CrdtSubscriber(id, this::processCommand));
        commands.subscribe(outCommands);
    }

    @Override
    public Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
        return LWWRegister::new;
    }

    @Override
    public String getId() {
        return id;
    }

    public T get() {
        return value;
    }

    public void set(T newValue) {
        if (! Objects.equals(value, newValue)) {
            doSet(newValue);
            commands.onNext(new SetCommand<>(
                    id,
                    value,
                    clock
            ));
        }
    }

    @SuppressWarnings("unchecked")
    private void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (SetCommand.class.equals(clazz)) {
            final SetCommand<T> setCommand = (SetCommand<T>) command;
            if (clock.compareTo(setCommand.getClock()) < 0) {
                clock = clock.merge(setCommand.getClock());
                doSet(setCommand.getValue());
            }
        }
    }

    private void doSet(T value) {
        this.value = value;
        clock = clock.increment();
    }

    static final class SetCommand<T> extends CrdtCommand {

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
