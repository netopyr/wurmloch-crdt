package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import javaslang.control.Option;

import java.util.UUID;

abstract class AbstractReplica implements Replica {

    private final String id;
    protected final Subject<CrdtCommand> inCommands = PublishSubject.create();
    private final Subject<CrdtCommand> outCommands = ReplaySubject.create();

    protected Map<String, Crdt> crdts = HashMap.empty();


    AbstractReplica() {
        this(UUID.randomUUID().toString());
    }
    AbstractReplica(String id) {
        this.id = id;
        inCommands.distinct().subscribe(outCommands);
    }


    @Override
    public String getId() {
        return id;
    }

    @Override
    public void register(Crdt crdt) {
        if (!crdts.containsKey(crdt.getId())) {
            crdts = crdts.put(crdt.getId(), crdt);
            final AddCrdtCommand command = new AddCrdtCommand(crdt);
            inCommands.onNext(command);
            crdt.onCommand().subscribe(inCommands::onNext);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Option<? extends Crdt> find(String id) {
        return crdts.get(id);
    }

    @Override
    public Observable<CrdtCommand> onCommands() {
        return outCommands;
    }

}
