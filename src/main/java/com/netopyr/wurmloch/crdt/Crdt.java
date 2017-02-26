package com.netopyr.wurmloch.crdt;

import org.reactivestreams.Publisher;

public interface Crdt<TYPE extends Crdt<TYPE, COMMAND>, COMMAND extends CrdtCommand> extends Publisher<COMMAND> {

    String getCrdtId();

    void subscribeTo(Publisher<? extends COMMAND> publisher);

    void connect(TYPE other);

}
