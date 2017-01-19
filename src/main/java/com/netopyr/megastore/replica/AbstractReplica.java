package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import com.netopyr.megastore.crdt.lwwregister.LWWRegister;
import com.netopyr.megastore.crdt.orset.ORSet;
import io.reactivex.Flowable;
import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.DisposableSubscriber;
import javaslang.Function4;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.control.Option;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.UUID;

class AbstractReplica implements Replica {

    private final String nodeId;
    protected final ReplayProcessor<CrdtCommand> commandProcessor = ReplayProcessor.create();
    private final Flowable<CrdtCommand> outCommands = commandProcessor.distinct();

    private Map<String, Crdt> crdts = HashMap.empty();


    AbstractReplica() {
        this(UUID.randomUUID().toString());
    }
    AbstractReplica(String nodeId) {
        this.nodeId = nodeId;
    }


    @Override
    public String getId() {
        return nodeId;
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
}
