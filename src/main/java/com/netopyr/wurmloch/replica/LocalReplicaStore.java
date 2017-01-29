package com.netopyr.wurmloch.replica;

import io.reactivex.disposables.Disposable;
import javaslang.collection.HashMap;
import javaslang.collection.Map;

public class LocalReplicaStore extends AbstractReplicaStore {

    private Map<LocalReplicaStore, Disposable> disposables = HashMap.empty();

    public LocalReplicaStore() {
        super();
    }
    public LocalReplicaStore(String id) {
        super(id);
    }


    public void connect(LocalReplicaStore other) {
        if (!disposables.containsKey(other)) {
            final ReplicaSubscriber subscriber = new ReplicaSubscriber();
            other.subscribe(subscriber);
            disposables = disposables.put(other, subscriber);
            other.connect(this);
        }
    }

    public void disconnect(LocalReplicaStore other) {
        disposables.get(other).forEach(
                disposable -> {
                    disposables = disposables.remove(other);
                    disposable.dispose();
                    other.disconnect(this);
                }
        );
    }

}
