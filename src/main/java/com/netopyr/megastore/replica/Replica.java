package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.crdt.lwwregister.LWWRegister;
import com.netopyr.megastore.crdt.orset.ORSet;
import javaslang.Function4;
import javaslang.control.Option;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.UUID;

public interface Replica extends Publisher<CrdtCommand> {

    String getId();

    Option<? extends Crdt> findCrdt(String crdtId);

    default <T extends Crdt> T createCrdt(Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, T> factory) {
        return createCrdt(factory, UUID.randomUUID().toString());
    }
    <T extends Crdt> T createCrdt(Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, T> factory, String id);

    default <T> LWWRegister<T> createLWWRegister() {
        return createLWWRegister(UUID.randomUUID().toString());
    }
    <T> LWWRegister<T> createLWWRegister(String id);

    default <T> ORSet<T> createORSet() {
        return createORSet(UUID.randomUUID().toString());
    }
    <T> ORSet<T> createORSet(String id);
}
