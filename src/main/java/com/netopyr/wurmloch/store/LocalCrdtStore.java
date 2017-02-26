package com.netopyr.wurmloch.store;

import com.netopyr.wurmloch.crdt.CrdtCommand;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.processors.BehaviorProcessor;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.reactivestreams.Processor;

public class LocalCrdtStore extends CrdtStore {

    private Map<LocalCrdtStore, LocalCrdtStoreSubscriber> subscribers = HashMap.empty();

    public LocalCrdtStore() {
        super();
    }

    public LocalCrdtStore(String id) {
        super(id);
    }


    public void connect(LocalCrdtStore other) {
        if (!subscribers.containsKey(other)) {
            final LocalCrdtStoreSubscriber subscriber = new LocalCrdtStoreSubscriber();
            other.subscribe(subscriber);
            subscribers = subscribers.put(other, subscriber);
            other.connect(this);
        }
    }

    public void disconnect(LocalCrdtStore other) {
        subscribers.get(other).peek(
                subscriber -> {
                    subscriber.dispose();
                    subscribers = subscribers.remove(other);
                    other.disconnect(this);
                }
        );
    }

    private class LocalCrdtStoreSubscriber extends CrdtStoreSubscriber implements Disposable {

        private final Processor<Boolean, Boolean> cancelProcessor = BehaviorProcessor.create();
        private boolean connected = true;

        @Override
        public void onNext(CrdtDefinition definition) {
            final Flowable<? extends CrdtCommand> publisher = Flowable.fromPublisher(definition.getPublisher()).takeUntil(cancelProcessor);
            final CrdtDefinition mappedDefinition = new CrdtDefinition(definition.getCrdtId(), definition.getCrdtClass(), publisher);
            super.onNext(mappedDefinition);
        }

        @Override
        public void dispose() {
            cancelProcessor.onNext(true);
            cancelProcessor.onComplete();

            connected = false;
        }

        @Override
        public boolean isDisposed() {
            return !connected;
        }
    }
}
