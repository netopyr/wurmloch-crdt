package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.crdt.CrdtSubscriber;
import com.netopyr.megastore.vectorclock.VectorClock;
import io.reactivex.processors.PublishProcessor;
import javaslang.Function4;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

public class LWWRegister<T> implements Crdt {

    private final String nodeId;
    private final String id;
    private final Processor<CrdtCommand, CrdtCommand> commands = PublishProcessor.create();

    private T value;
    private VectorClock clock = new VectorClock();

    public LWWRegister(String nodeId, String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        this.nodeId = Objects.requireNonNull(nodeId, "NodeId must not be null");
        this.id = Objects.requireNonNull(id, "Id must not be null");
        inCommands.subscribe(new CrdtSubscriber(id, this::processCommand));
        commands.subscribe(outCommands);
    }

    @Override
    public String getId() {
        return id;
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

    private void doSet(T value) {
        this.value = value;
        clock.increment(nodeId);
    }

    @SuppressWarnings("unchecked")
    private void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (SetCommand.class.equals(clazz)) {
            doSet((SetCommand) command);
        }
    }

    private void doSet(SetCommand<T> setCommand) {
        final int clockComparison = clock.compareTo(setCommand.getClock());
        if (clockComparison < 0 || (clockComparison == 0 & this.nodeId.compareTo(setCommand.getReplicaId()) > 0)) {
            clock = clock.merge(setCommand.getClock());
            doSet(setCommand.getValue());
        }
    }
}
