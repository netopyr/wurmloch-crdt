package com.netopyr.megastore.replica;

import io.reactivex.disposables.Disposable;
import javaslang.collection.HashMap;
import javaslang.collection.Map;

public class LocalReplica extends AbstractReplica {

    private Map<LocalReplica, Disposable> disposables = HashMap.empty();

    public LocalReplica() {
        super();
    }
    public LocalReplica(String id) {
        super(id);
    }


    public void connect(LocalReplica other) {
        if (!disposables.containsKey(other)) {
            final ReplicaSubscriber subscriber = new ReplicaSubscriber();
            other.subscribe(subscriber);
            disposables = disposables.put(other, subscriber);
            other.connect(this);
        }
    }

    public void disconnect(LocalReplica other) {
        disposables.get(other).forEach(
                disposable -> {
                    disposables = disposables.remove(other);
                    disposable.dispose();
                    other.disconnect(this);
                }
        );
    }

}
