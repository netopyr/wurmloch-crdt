package com.netopyr.wurmloch.store;

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

public interface CrdtStore extends Publisher<CrdtCommand> {

    Option<? extends Crdt> findCrdt(String crdtId);


    default <T extends Crdt> T createCrdt(Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, T> factory) {
        return createCrdt(factory, UUID.randomUUID().toString());
    }
    <T extends Crdt> T createCrdt(Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, T> factory, String id);


    default <T> LWWRegister<T> createLWWRegister() {
        return createLWWRegister(UUID.randomUUID().toString());
    }
    <T> LWWRegister<T> createLWWRegister(String id);

    @SuppressWarnings("unchecked")
    default <T> Option<LWWRegister<T>> findLWWRegister(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof LWWRegister? Option.of((LWWRegister<T>) crtd) : Option.none());
    }

    default <T>MVRegister<T> createMVRegister() {
        return createMVRegister(UUID.randomUUID().toString());
    }
    <T>MVRegister<T> createMVRegister(String id);

    @SuppressWarnings("unchecked")
    default <T> Option<MVRegister<T>> findMVRegister(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof MVRegister? Option.of((MVRegister<T>) crtd) : Option.none());
    }


    default GCounter createGCounter() {
        return createGCounter(UUID.randomUUID().toString());
    }
    GCounter createGCounter(String id);

    default Option<GCounter> findGCounter(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof GCounter? Option.of((GCounter) crtd) : Option.none());
    }


    default PNCounter createPNCounter() {
        return createPNCounter(UUID.randomUUID().toString());
    }
    PNCounter createPNCounter(String id);

    default Option<PNCounter> findPNCounter(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof PNCounter? Option.of((PNCounter) crtd) : Option.none());
    }


    default <E> GSet<E> createGSet() {
        return createGSet(UUID.randomUUID().toString());
    }
    <E> GSet<E> createGSet(String id);

    @SuppressWarnings("unchecked")
    default <E> Option<GSet<E>> findGSet(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof GSet? Option.of((GSet<E>) crtd) : Option.none());
    }


    default <E> ORSet<E> createORSet() {
        return createORSet(UUID.randomUUID().toString());
    }
    <E> ORSet<E> createORSet(String id);

    @SuppressWarnings("unchecked")
    default <E> Option<ORSet<E>> findORSet(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof ORSet? Option.of((ORSet<E>) crtd) : Option.none());
    }


    default <E> RGA<E> createRGA() {
        return createRGA(UUID.randomUUID().toString());
    }
    <E> RGA<E> createRGA(String id);

    @SuppressWarnings("unchecked")
    default <E> Option<RGA<E>> findRGA(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof RGA? Option.of((RGA<E>) crtd) : Option.none());
    }
}
