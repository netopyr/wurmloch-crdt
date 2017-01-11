package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.replica.Replica;
import com.netopyr.megastore.vectorclock.VectorClock;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.util.Objects;
import java.util.function.BiFunction;

public class LWWRegister<T> implements Crdt {

    private final String replicaId;
    private final String id;
    private final Subject<CrdtCommand> commands = PublishSubject.create();

    private T value;
    private VectorClock clock = new VectorClock();

    public LWWRegister(Replica replica, String id) {
        this.replicaId = Objects.requireNonNull(replica, "Replica must not be null").getId();
        this.id = Objects.requireNonNull(id, "ID must not be null");
        replica.onCommands(this).subscribe(this::processCommand);
        replica.register(this);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Observable<CrdtCommand> onCommand() {
        return commands;
    }

    @Override
    public BiFunction<Replica, String, Crdt> getFactory() {
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
                    replicaId,
                    value,
                    clock
            ));
        }
    }

    private void doSet(T value) {
        this.value = value;
        clock.increment(replicaId);
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
        if (clockComparison < 0 || (clockComparison == 0 & this.replicaId.compareTo(setCommand.getReplicaId()) > 0)) {
            clock = clock.merge(setCommand.getClock());
            doSet(setCommand.getValue());
        }
    }


}
