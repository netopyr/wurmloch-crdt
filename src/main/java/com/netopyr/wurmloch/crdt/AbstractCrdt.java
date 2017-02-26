package com.netopyr.wurmloch.crdt;

import io.reactivex.Flowable;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractCrdt<TYPE extends AbstractCrdt<TYPE, COMMAND>, COMMAND extends CrdtCommand> implements Crdt<TYPE, COMMAND> {

    // fields
    protected final String nodeId;
    protected final String crdtId;
    protected final Processor<COMMAND, COMMAND> commands;


    // constructor
    public AbstractCrdt(String nodeId, String crdtId, Processor<COMMAND, COMMAND> commands) {
        this.nodeId = Objects.requireNonNull(nodeId, "NodeId must not be null");
        this.crdtId = Objects.requireNonNull(crdtId, "Id must not be null");
        this.commands = Objects.requireNonNull(commands, "Commands must not be null");
    }


    // crdt
    @Override
    public String getCrdtId() {
        return crdtId;
    }

    @Override
    public void subscribe(Subscriber<? super COMMAND> subscriber) {
        commands.subscribe(subscriber);
    }

    @Override
    public void subscribeTo(Publisher<? extends COMMAND> publisher) {
        Flowable.fromPublisher(publisher).onTerminateDetach().subscribe(command -> {
            if (processCommand(command)) {
                commands.onNext(command);
            }
        });
    }

    @Override
    public void connect(TYPE other) {
        if (!Objects.equals(crdtId, other.getCrdtId())) {
            throw new IllegalArgumentException("Ids do not match");
        }
        subscribeTo(other);
        other.subscribeTo(this);
    }

    protected abstract boolean processCommand(COMMAND command);

}
