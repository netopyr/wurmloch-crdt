package com.netopyr.wurmloch.store;

import io.reactivex.disposables.Disposable;
import javaslang.collection.HashMap;
import javaslang.collection.Map;

public class LocalCrdtStore extends AbstractCrdtStore {

    private Map<LocalCrdtStore, Disposable> disposables = HashMap.empty();

    public LocalCrdtStore() {
        super();
    }
    public LocalCrdtStore(String id) {
        super(id);
    }


    public void connect(LocalCrdtStore other) {
        if (!disposables.containsKey(other)) {
            final ReplicaSubscriber subscriber = new ReplicaSubscriber();
            other.subscribe(subscriber);
            disposables = disposables.put(other, subscriber);
            other.connect(this);
        }
    }

    public void disconnect(LocalCrdtStore other) {
        disposables.get(other).forEach(
                disposable -> {
                    disposables = disposables.remove(other);
                    disposable.dispose();
                    other.disconnect(this);
                }
        );
    }

}
