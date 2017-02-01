package com.netopyr.wurmloch.crdt;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.function.Consumer;

public class CrdtSubscriber implements Subscriber<CrdtCommand> {

    private final Consumer<CrdtCommand> consumer;

    private Subscription subscription;

    public CrdtSubscriber(Consumer<CrdtCommand> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = Objects.requireNonNull(subscription, "subscription must not be null");
        subscription.request(1L);
    }

    @Override
    public void onNext(CrdtCommand crdtCommand) {
        consumer.accept(crdtCommand);
        subscription.request(1L);
    }

    @Override
    public void onError(Throwable throwable) {
        // ignore
    }

    @Override
    public void onComplete() {
        // ignore
    }
}
