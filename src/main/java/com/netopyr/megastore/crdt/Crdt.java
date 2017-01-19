package com.netopyr.megastore.crdt;

import javaslang.Function4;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public interface Crdt {

    String getId();

    Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory();

}
