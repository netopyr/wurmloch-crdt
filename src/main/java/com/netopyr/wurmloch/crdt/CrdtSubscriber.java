package com.netopyr.wurmloch.crdt;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.function.Function;

public class CrdtSubscriber<T extends CrdtCommand> implements Subscriber<T> {

    private final Subscriber<T> commandQueue;
    private final Function<T, Boolean> processor;

    private Subscription subscription;

    public CrdtSubscriber(Subscriber<T> commandQueue, Function<T, Boolean> processor) {
        this.commandQueue = Objects.requireNonNull(commandQueue, "CommandQueue must not be null");
        this.processor = Objects.requireNonNull(processor, "Processor must not be null");
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = Objects.requireNonNull(subscription, "subscription must not be null");
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T command) {
        if (processor.apply(command)) {
            commandQueue.onNext(command);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        subscription.cancel();
    }

    @Override
    public void onComplete() {
        subscription.cancel();
    }
}
