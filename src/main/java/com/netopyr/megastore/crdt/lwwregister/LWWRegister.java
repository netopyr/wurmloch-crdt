package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.crdt.AbstractCrdt;
import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.vectorclock.VectorClock;
import javaslang.Function4;
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
            commands.onNext(new SetLWWCommand<>(
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
        if (SetLWWCommand.class.equals(clazz)) {
            final SetLWWCommand<T> setLWWCommand = (SetLWWCommand<T>) command;
            final int clockComparison = clock.compareTo(setLWWCommand.getClock());
            if (clockComparison < 0 || (clockComparison == 0 & this.nodeId.compareTo(setLWWCommand.getNodeId()) > 0)) {
                clock = clock.merge(setLWWCommand.getClock());
                doSet(setLWWCommand.getValue());
            }
        }
    }

    private void doSet(T value) {
        this.value = value;
        clock = clock.increment(nodeId);
    }
}
