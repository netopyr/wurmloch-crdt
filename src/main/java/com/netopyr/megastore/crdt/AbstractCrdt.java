package com.netopyr.megastore.crdt;

import io.reactivex.processors.PublishProcessor;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Objects;

public abstract class AbstractCrdt implements Crdt {

    protected final String id;
    protected final Processor<CrdtCommand, CrdtCommand> commands = PublishProcessor.create();

    protected AbstractCrdt(String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        this.id = Objects.requireNonNull(id, "Id must not be null");
        inCommands = Objects.requireNonNull(inCommands, "InCommands must not be null");
        outCommands = Objects.requireNonNull(outCommands, "OutCommands must not be null");
        inCommands.subscribe(new CrdtSubscriber(id, this::processCommand));
        commands.subscribe(outCommands);
    }

    @Override
    public String getId() {
        return id;
    }

    protected abstract void processCommand(CrdtCommand command);

}
