package com.netopyr.megastore.crdt;

import com.netopyr.megastore.replica.Replica;
import io.reactivex.Observable;

import java.util.function.BiFunction;

public interface Crdt {

    String getId();

    Observable<CrdtCommand> onCommand();

    BiFunction<Replica, String, Crdt> getFactory();

}
