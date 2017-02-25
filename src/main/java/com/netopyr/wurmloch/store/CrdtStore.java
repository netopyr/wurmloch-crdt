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
import io.reactivex.processors.PublishProcessor;
import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.DisposableSubscriber;
import javaslang.collection.HashMap;
import javaslang.collection.HashSet;
import javaslang.collection.Map;
import javaslang.collection.Set;
import javaslang.control.Option;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;

@SuppressWarnings("WeakerAccess")
public class CrdtStore implements Publisher<CrdtCommand> {

    private final String nodeId;

    private Set<CrdtCommand> commands = HashSet.empty();
    private final PublishProcessor<CrdtCommand> inCommandsEntry = PublishProcessor.create();
    private final PublishProcessor<CrdtCommand> inCommandsExit = PublishProcessor.create();
    private final PublishProcessor<CrdtCommand> outCommandsEntry = PublishProcessor.create();
    private final ReplayProcessor<CrdtCommand> outCommandsExit = ReplayProcessor.create();

    private Map<String, Crdt> crdts = HashMap.empty();

    public CrdtStore() {
        this(UUID.randomUUID().toString());
    }
    public CrdtStore(String nodeId) {
        this.nodeId = nodeId;
        inCommandsEntry
                .filter(command -> !commands.contains(command))
                .doOnNext(command -> commands = commands.add(command))
                .subscribe(inCommandsExit);
        outCommandsEntry
                .doOnNext(command -> commands = commands.add(command))
                .subscribe(outCommandsExit);
        inCommandsExit.subscribe(outCommandsEntry);
    }

    public Option<? extends Crdt> findCrdt(String crdtId) {
        return crdts.get(crdtId);
    }

    public <T extends Crdt> T createCrdt(BiFunction<String, String, T> factory) {
        return createCrdt(factory, UUID.randomUUID().toString());
    }
    public <T extends Crdt> T createCrdt(BiFunction<String, String, T> factory, String id) {
        Objects.requireNonNull(factory, "factory must not be null");
        Objects.requireNonNull(id, "id must not be null");
        final T result = factory.apply(nodeId, id);
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
        return option.flatMap(crtd -> crtd instanceof LWWRegister? Option.of((LWWRegister<T>) crtd) : Option.none());
    }

    public <T>MVRegister<T> createMVRegister() {
        return createMVRegister(UUID.randomUUID().toString());
    }
    public <T>MVRegister<T> createMVRegister(String id) {
        Objects.requireNonNull(id, "id must not be null");
        final MVRegister<T> result = new MVRegister<>(nodeId, id);
        register(result);
        return result;
    }

    @SuppressWarnings("unchecked")
    public <T> Option<MVRegister<T>> findMVRegister(String crtdId) {
        final Option<? extends Crdt> option = findCrdt(crtdId);
        return option.flatMap(crtd -> crtd instanceof MVRegister? Option.of((MVRegister<T>) crtd) : Option.none());
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
        return option.flatMap(crtd -> crtd instanceof GCounter? Option.of((GCounter) crtd) : Option.none());
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
        return option.flatMap(crtd -> crtd instanceof PNCounter? Option.of((PNCounter) crtd) : Option.none());
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
        return option.flatMap(crtd -> crtd instanceof GSet? Option.of((GSet<E>) crtd) : Option.none());
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
        return option.flatMap(crtd -> crtd instanceof ORSet? Option.of((ORSet<E>) crtd) : Option.none());
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
        return option.flatMap(crtd -> crtd instanceof RGA? Option.of((RGA<E>) crtd) : Option.none());
    }

    @SuppressWarnings("unchecked")
    private void connect(Crdt crdt) {
        crdt.subscribe(outCommandsEntry);
        crdt.subscribeTo(
                inCommandsExit
                        .filter(command -> Objects.equals(crdt.getCrdtId(), command.getCrdtId()))
                        .filter(command -> !(command instanceof AddCrdtCommand))
        );
    }

    private void register(Crdt crdt) {
        connect(crdt);
        crdts = crdts.put(crdt.getCrdtId(), crdt);
        final AddCrdtCommand command = new AddCrdtCommand(crdt);
        outCommandsEntry.onNext(command);
    }

    @Override
    public void subscribe(Subscriber<? super CrdtCommand> subscriber) {
        outCommandsExit.subscribe(subscriber);
    }

    protected class ReplicaSubscriber extends DisposableSubscriber<CrdtCommand> {

        @Override
        public void onNext(CrdtCommand command) {
            if (AddCrdtCommand.class.equals(command.getClass())) {
                final String crdtId = command.getCrdtId();
                if (findCrdt(crdtId).isEmpty()) {
                    final Crdt crdt = ((AddCrdtCommand) command).getFactory().apply(
                            nodeId,
                            crdtId
                    );
                    connect(crdt);
                    crdts = crdts.put(crdt.getCrdtId(), crdt);
                }
            }
            inCommandsEntry.onNext(command);
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


    static final class AddCrdtCommand extends CrdtCommand {

        private final Class<? extends Crdt> crdtClass;
        private final BiFunction<String, String, Crdt> factory;

        @SuppressWarnings("unchecked")
        AddCrdtCommand(Crdt crdt) {
            super(crdt.getCrdtId());
            this.crdtClass = crdt.getClass();
            this.factory = crdt.getFactory();
        }

        BiFunction<String, String, Crdt> getFactory() {
            return factory;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            AddCrdtCommand that = (AddCrdtCommand) o;

            return new EqualsBuilder()
                    .appendSuper(super.equals(o))
                    .append(crdtClass, that.crdtClass)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .appendSuper(super.hashCode())
                    .append(crdtClass)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("crdtClass", crdtClass)
                    .toString();
        }
    }

}
