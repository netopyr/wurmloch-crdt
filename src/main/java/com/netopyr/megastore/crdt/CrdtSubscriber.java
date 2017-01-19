package com.netopyr.megastore.crdt;

import io.reactivex.subscribers.DisposableSubscriber;

import java.util.Objects;
import java.util.function.Consumer;

public class CrdtSubscriber extends DisposableSubscriber<CrdtCommand> {

    private final String crdtId;
    private final Consumer<CrdtCommand> consumer;

    public CrdtSubscriber(String crdtId, Consumer<CrdtCommand> consumer) {
        this.crdtId = crdtId;
        this.consumer = consumer;
    }

    @Override
    public void onNext(CrdtCommand crdtCommand) {
        if (Objects.equals(crdtCommand.getCrdtId(), crdtId)) {
            consumer.accept(crdtCommand);
        }
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
