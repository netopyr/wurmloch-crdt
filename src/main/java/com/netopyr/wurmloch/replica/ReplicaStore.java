package com.netopyr.wurmloch.replica;

import com.netopyr.wurmloch.crdt.Crdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import com.netopyr.wurmloch.crdt.GCounter;
import com.netopyr.wurmloch.crdt.GSet;
import com.netopyr.wurmloch.crdt.LWWRegister;
import com.netopyr.wurmloch.crdt.MVRegister;
import com.netopyr.wurmloch.crdt.ORSet;
import com.netopyr.wurmloch.crdt.PNCounter;
import com.netopyr.wurmloch.crdt.RGA;
import javaslang.Function4;
import javaslang.control.Option;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.UUID;

public interface ReplicaStore extends Publisher<CrdtCommand> {

    Option<? extends Crdt> findCrdt(String crdtId);


    default <T extends Crdt> T createCrdt(Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, T> factory) {
        return createCrdt(factory, UUID.randomUUID().toString());
    }
    <T extends Crdt> T createCrdt(Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, T> factory, String id);


    default <T> LWWRegister<T> createLWWRegister() {
        return createLWWRegister(UUID.randomUUID().toString());
    }
    <T> LWWRegister<T> createLWWRegister(String id);


    default <T>MVRegister<T> createMVRegister() {
        return createMVRegister(UUID.randomUUID().toString());
    }
    <T>MVRegister<T> createMVRegister(String id);


    default GCounter createGCounter() {
        return createGCounter(UUID.randomUUID().toString());
    }
    GCounter createGCounter(String id);


    default PNCounter createPNCounter() {
        return createPNCounter(UUID.randomUUID().toString());
    }
    PNCounter createPNCounter(String id);


    default <T> GSet<T> createGSet() {
        return createGSet(UUID.randomUUID().toString());
    }
    <T> GSet<T> createGSet(String id);


    default <T> ORSet<T> createORSet() {
        return createORSet(UUID.randomUUID().toString());
    }
    <T> ORSet<T> createORSet(String id);


    default <T> RGA<T> createRGA() {
        return createRGA(UUID.randomUUID().toString());
    }
    <T> RGA<T> createRGA(String id);
}
