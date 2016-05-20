package com.netopyr.megastore.crdt;

import rx.Observable;

public interface Crdt {

    String getId();

    Observable<CrdtCommand> onCommands();

}
