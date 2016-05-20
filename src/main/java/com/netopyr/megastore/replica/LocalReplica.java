package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import rx.Observable;
import rx.Subscription;
import rx.subjects.ReplaySubject;
import rx.subjects.Subject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LocalReplica implements Replica {

    private final Map<LocalReplica, Subscription> subscriptions = new HashMap<>();
    private final Map<String, Crdt> crdts = new HashMap<>();
    private final Subject<CrdtCommand, CrdtCommand> commands = ReplaySubject.create();

    @Override
    public void register(Crdt crdt) {
        if (! crdts.containsKey(crdt.getId())) {
            crdts.put(crdt.getId(), crdt);
            final AddCrdtCommand command = new AddCrdtCommand(crdt.getId(), crdt.getClass());
            commands.onNext(command);
            crdt.onCommands().subscribe(this::processCommand);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<? extends Crdt> find(String id) {
        return Optional.ofNullable(crdts.get(id));
    }

    @Override
    public Observable<CrdtCommand> onCommands() {
        return commands.distinct().cache();
    }

    public void connect(LocalReplica other) {
        if (!subscriptions.containsKey(other)) {
            final Subscription subscription = other.onCommands().subscribe(this::processCommand);
            subscriptions.put(other, subscription);
            other.connect(this);
        }
    }

    public void disconnect(LocalReplica other) {
        final Subscription subscription = subscriptions.get(other);
        if (subscription != null) {
            subscriptions.remove(other);
            subscription.unsubscribe();
            other.disconnect(this);
        }
    }

    private void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (AddCrdtCommand.class.equals(clazz)) {
            doAddCrdt((AddCrdtCommand)command);
        } else {
            doCrdtCommand(command);
        }
    }

    private void doAddCrdt(AddCrdtCommand command) {
        try {
            final Constructor<? extends Crdt> constructor = command.getCrdtClass().getConstructor(Replica.class, String.class);
            final Crdt crdt = constructor.newInstance(this, command.getCrdtId());
            crdts.put(crdt.getId(), crdt);
            commands.onNext(command);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    private void doCrdtCommand(CrdtCommand command) {
        commands.onNext(command);
    }

}
