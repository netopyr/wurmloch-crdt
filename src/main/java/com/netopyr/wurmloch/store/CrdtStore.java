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
import io.reactivex.Flowable;
import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.DefaultSubscriber;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.control.Option;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;

@SuppressWarnings({"WeakerAccess", "unused", "SameParameterValue"})
public class CrdtStore implements Publisher<CrdtDefinition> {

    // fields
    private final String nodeId;
    private final Processor<CrdtDefinition, CrdtDefinition> definitions = ReplayProcessor.create();

    private Map<String, Crdt> crdts = HashMap.empty();
    private Map<Class<? extends Crdt>, BiFunction<String, String, ? extends Crdt>> factories = HashMap.empty();


    // constructor
    public CrdtStore() {
        this(UUID.randomUUID().toString());
    }

    public CrdtStore(String nodeId) {
        this.nodeId = nodeId;
        registerDefaultFactories();
    }

    @SuppressWarnings("unchecked")
    private void registerDefaultFactories() {
        registerFactory(LWWRegister.class, (BiFunction<String, String, LWWRegister>) LWWRegister::new);
        registerFactory(MVRegister.class, (BiFunction<String, String, MVRegister>) MVRegister::new);
        registerFactory(GCounter.class, GCounter::new);
        registerFactory(PNCounter.class, PNCounter::new);
        registerFactory(GSet.class, (nodeId, crdtId) -> new GSet(crdtId));
        registerFactory(ORSet.class, (nodeId, crdtId) -> new ORSet(crdtId));
        registerFactory(RGA.class, (BiFunction<String, String, RGA>) RGA::new);
    }


    // find- and factory-methods
    public <T extends Crdt<T, ? extends CrdtCommand>> void registerFactory(Class<T> crdtClass, BiFunction<String, String, T> builder) {
        factories = factories.put(crdtClass, builder);
    }

    public Option<? extends Crdt> findCrdt(String crdtId) {
        return crdts.get(crdtId);
    }

    public <T extends Crdt> T createCrdt(Class<T> crdtClass) {
        return createCrdt(crdtClass, UUID.randomUUID().toString());
    }

    @SuppressWarnings("unchecked")
    public <T extends Crdt> T createCrdt(Class<T> crdtClass, String crdtId) {
        Objects.requireNonNull(crdtClass, "CrdtClass must not be null");
        Objects.requireNonNull(crdtId, "CrdtId must not be null");
        final Option<BiFunction<String, String, ? extends Crdt>> factory = factories.get(crdtClass);
        if (factory.isEmpty()) {
            throw new IllegalArgumentException("Factory for class " + crdtClass + " not defined");
        }
        final T result = (T) factory.get().apply(nodeId, crdtId);
        register(result);
        return result;
    }


    public <T> LWWRegister<T> createLWWRegister() {
        return createLWWRegister(UUID.randomUUID().toString());
    }

    public <T> LWWRegister<T> createLWWRegister(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final LWWRegister<T> result = new LWWRegister<>(nodeId, id);
        register(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    public <T> Option<LWWRegister<T>> findLWWRegister(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof LWWRegister ? Option.of((LWWRegister<T>) crtd) : Option.none());
    }

    public <T> MVRegister<T> createMVRegister() {
        return createMVRegister(UUID.randomUUID().toString());
    }

    public <T> MVRegister<T> createMVRegister(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final MVRegister<T> result = new MVRegister<>(nodeId, id);
        register(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    public <T> Option<MVRegister<T>> findMVRegister(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof MVRegister ? Option.of((MVRegister<T>) crtd) : Option.none());
    }


    public GCounter createGCounter() {
        return createGCounter(UUID.randomUUID().toString());
    }

    public GCounter createGCounter(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final GCounter result = new GCounter(nodeId, id);
        register(result);
        return result;
    }

    public Option<GCounter> findGCounter(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof GCounter ? Option.of((GCounter) crtd) : Option.none());
    }


    public PNCounter createPNCounter() {
        return createPNCounter(UUID.randomUUID().toString());
    }

    public PNCounter createPNCounter(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final PNCounter result = new PNCounter(nodeId, id);
        register(result);
        return result;
    }

    public Option<PNCounter> findPNCounter(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof PNCounter ? Option.of((PNCounter) crtd) : Option.none());
    }


    public <E> GSet<E> createGSet() {
        return createGSet(UUID.randomUUID().toString());
    }

    public <E> GSet<E> createGSet(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final GSet<E> result = new GSet<>(id);
        register(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    public <E> Option<GSet<E>> findGSet(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof GSet ? Option.of((GSet<E>) crtd) : Option.none());
    }


    public <E> ORSet<E> createORSet() {
        return createORSet(UUID.randomUUID().toString());
    }

    public <E> ORSet<E> createORSet(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final ORSet<E> result = new ORSet<>(id);
        register(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    public <E> Option<ORSet<E>> findORSet(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof ORSet ? Option.of((ORSet<E>) crtd) : Option.none());
    }


    public <E> RGA<E> createRGA() {
        return createRGA(UUID.randomUUID().toString());
    }

    public <E> RGA<E> createRGA(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final RGA<E> result = new RGA<>(nodeId, id);
        register(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    public <E> Option<RGA<E>> findRGA(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof RGA ? Option.of((RGA<E>) crtd) : Option.none());
    }


    // implementation
    @SuppressWarnings("unchecked")
    private void connect(CrdtStore that) {
        this.subscribeTo(that);
        that.subscribeTo(this);
    }

    @SuppressWarnings("unchecked")
    private void register(Crdt crdt) {
        crdts = crdts.put(crdt.getCrdtId(), crdt);
        definitions.onNext(new CrdtDefinition(crdt.getCrdtId(), crdt.getClass(), crdt));
    }


    // subscribe
    @Override
    public void subscribe(Subscriber<? super CrdtDefinition> subscriber) {
        definitions.subscribe(subscriber);
    }

    public void subscribeTo(Publisher<? extends CrdtDefinition> publisher) {
        Flowable.fromPublisher(publisher).onTerminateDetach().subscribe(new CrdtStoreSubscriber());
    }


    protected class CrdtStoreSubscriber extends DefaultSubscriber<CrdtDefinition> {

        @SuppressWarnings("unchecked")
        @Override
        public void onNext(CrdtDefinition definition) {
            final String crdtId = definition.getCrdtId();
            final Option<? extends Crdt> existingCrdt = findCrdt(crdtId);
            if (existingCrdt.isDefined()) {
                existingCrdt.get().subscribeTo(definition.getPublisher());
            } else {
                final Class<? extends Crdt> crdtClass = definition.getCrdtClass();
                factories.get(crdtClass)
                        .map(factory -> factory.apply(nodeId, crdtId))
                        .peek(CrdtStore.this::register)
                        .peek(crdt -> crdt.subscribeTo(definition.getPublisher()));
            }
        }

        @Override
        public void onError(Throwable throwable) {
            cancel();
        }

        @Override
        public void onComplete() {
            cancel();
        }
    }
}
