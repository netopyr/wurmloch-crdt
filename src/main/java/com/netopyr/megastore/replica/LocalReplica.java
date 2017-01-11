package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import io.reactivex.disposables.Disposable;
import javaslang.collection.HashMap;
import javaslang.collection.Map;

public class LocalReplica extends AbstractReplica {

    private Map<LocalReplica, Disposable> subscriptions = HashMap.empty();

    public LocalReplica() {
        super();
    }
    public LocalReplica(String id) {
        super(id);
    }


    public void connect(LocalReplica other) {
        if (!subscriptions.containsKey(other)) {
            final Disposable subscription = other.onCommands().subscribe(this::processCommand);
            subscriptions = subscriptions.put(other, subscription);
            other.connect(this);
        }
    }

    public void disconnect(LocalReplica other) {
        subscriptions.get(other).forEach(
                disposable -> {
                    subscriptions = subscriptions.remove(other);
                    disposable.dispose();
                    other.disconnect(this);
                }
        );
    }

    private void processCommand(CrdtCommand command) {
        final Class<? extends CrdtCommand> clazz = command.getClass();
        if (AddCrdtCommand.class.equals(clazz)) {
            doAddCrdt((AddCrdtCommand)command);
        } else {
            inCommands.onNext(command);
        }
    }

    private void doAddCrdt(AddCrdtCommand command) {
        final Crdt crdt = command.getFactory().apply(this, command.getCrdtId());
        crdts = crdts.put(crdt.getId(), crdt);
        inCommands.onNext(command);
    }
}
