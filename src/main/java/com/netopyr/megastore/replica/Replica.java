package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import rx.Observable;

import java.util.Optional;

public interface Replica {

    void register(Crdt crdt);

    Optional<? extends Crdt> find(String id);

    Observable<CrdtCommand> onCommands();

    default Observable<CrdtCommand> onCommands(Crdt crdt) {
        return onCommands().filter(command -> crdt.getId() == null || crdt.getId().equals(command.getCrdtId()));
    }
}
