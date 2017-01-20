package com.netopyr.wurmloch.replica;

import com.netopyr.wurmloch.crdt.Crdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import com.netopyr.wurmloch.crdt.LWWRegister;
import com.netopyr.wurmloch.crdt.ORSet;
import io.reactivex.Flowable;
import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.DisposableSubscriber;
import javaslang.Function4;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.control.Option;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.UUID;

class AbstractReplica implements Replica {

    private final String nodeId;
    private final ReplayProcessor<CrdtCommand> commandProcessor = ReplayProcessor.create();
    private final Flowable<CrdtCommand> outCommands = commandProcessor.distinct();

    private Map<String, Crdt> crdts = HashMap.empty();


    AbstractReplica() {
        this(UUID.randomUUID().toString());
    }
    AbstractReplica(String nodeId) {
        this.nodeId = nodeId;
    }


    @SuppressWarnings("unchecked")
    @Override
    public Option<? extends Crdt> findCrdt(String crdtId) {
        return crdts.get(crdtId);
    }

    @Override
    public <T extends Crdt> T createCrdt(Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, T> factory, String id) {
        final T result = factory.apply(nodeId, id, outCommands, new ReplicaSubscriber());
        register(result);
        return result;
    }

    @Override
    public <T> LWWRegister<T> createLWWRegister(String id) {
        final LWWRegister<T> result = new LWWRegister<>(nodeId, id, outCommands, new ReplicaSubscriber());
        register(result);
        return result;
    }

    @Override
    public <T> ORSet<T> createORSet(String id) {
        final ORSet<T> result = new ORSet<>(id, outCommands, new ReplicaSubscriber());
        register(result);
        return result;
    }

    private void register(Crdt crdt) {
        crdts = crdts.put(crdt.getId(), crdt);
        final AddCrdtCommand command = new AddCrdtCommand(crdt);
        commandProcessor.onNext(command);
    }

    @Override
    public void subscribe(Subscriber<? super CrdtCommand> subscriber) {
        outCommands.subscribe(subscriber);
    }

    protected class ReplicaSubscriber extends DisposableSubscriber<CrdtCommand> {

        @Override
        public void onNext(CrdtCommand command) {
            if (AddCrdtCommand.class.equals(command.getClass())) {
                final Crdt crdt = ((AddCrdtCommand)command).getFactory().apply(nodeId, command.getCrdtId(), outCommands, new ReplicaSubscriber());
                crdts = crdts.put(crdt.getId(), crdt);
            }
            commandProcessor.onNext(command);
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
        private final Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> factory;

        AddCrdtCommand(Crdt crdt) {
            super(crdt.getId());
            this.crdtClass = crdt.getClass();
            this.factory = crdt.getFactory();
        }

        Function4<String, String, Publisher<? extends CrdtCommand>, Subscriber<? super CrdtCommand>, Crdt> getFactory() {
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
