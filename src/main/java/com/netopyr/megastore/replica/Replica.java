package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import io.reactivex.Observable;
import javaslang.control.Option;

import java.util.Objects;

public interface Replica {

    String getId();

    void register(Crdt crdt);

    Option<? extends Crdt> find(String id);

    Observable<CrdtCommand> onCommands();

    default Observable<CrdtCommand> onCommands(Crdt crdt) {
        return onCommands().filter(command -> Objects.equals(crdt.getId(), command.getCrdtId()));
    }
}
