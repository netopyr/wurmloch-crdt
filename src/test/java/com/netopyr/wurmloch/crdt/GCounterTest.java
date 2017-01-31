package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import javaslang.Tuple;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.hamcrest.CustomMatcher;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GCounterTest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String CRDT_ID = "ID_1";


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new GCounter(null, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new GCounter(NODE_ID_1, null, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullPublisherShouldThrow() {
        new GCounter(NODE_ID_1, CRDT_ID, null, mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullSubscriberShouldThrow() {
        new GCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), null);
    }

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByNullShouldFail() {
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
        counter.increment(0L);
    }

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByANegativeValueShouldFail() {
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
        counter.increment(-1L);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldInitializeWithWaitingInCommands() {
        // given
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final GCounter counter1 = new GCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), outCommands1);
        counter1.increment(42L);

        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        inCommands2.onNext(outCommands1.values().get(0));

        // when
        final GCounter counter2 = new GCounter(NODE_ID_2, CRDT_ID, inCommands2, mock(Subscriber.class));

        // then
        assertThat(counter2.get(), is(42L));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldGetAndIncrementTheValue() {
        // given
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));

        // when
        final Long v0 = counter.get();

        // then
        assertThat(v0, is(0L));

        // when
        counter.increment();
        final Long v1 = counter.get();

        // then
        assertThat(v1, is(1L));

        // when
        counter.increment(42L);
        final Long v2 = counter.get();

        // then
        assertThat(v2, is(43L));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldSendCommandsOnUpdates() {
        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), subscriber);

        // when
        counter.increment();

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new UpdateCommandMatcher(CRDT_ID, HashMap.of(NODE_ID_1, 1L))
        ));

        // when
        counter.increment(42L);

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new UpdateCommandMatcher(CRDT_ID, HashMap.of(NODE_ID_1, 1L)),
                new UpdateCommandMatcher(CRDT_ID, HashMap.of(NODE_ID_1, 43L))
        ));
    }


    @Test
    public void itShouldAcceptIncrementsFromReceivedCommands() {
        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final GCounter counter1 = new GCounter(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final GCounter counter2 = new GCounter(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        counter1.increment();
        inCommands2.onNext(outCommands1.values().get(0));

        // then
        assertThat(counter2.get(), is(1L));
        outCommands2.assertNoValues();
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();

        // when
        counter2.increment();
        inCommands1.onNext(outCommands2.values().get(0));

        // then
        assertThat(counter1.get(), is(2L));
        assertThat(counter2.get(), is(2L));
        assertThat(outCommands1.valueCount(), is(1));
        outCommands1.assertNotComplete();
        outCommands1.assertNoErrors();
    }


    @Test
    public void itShouldIncludeOtherNodeValuesInCommands() {
        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final GCounter counter1 = new GCounter(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final GCounter counter2 = new GCounter(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        counter1.increment(3);
        inCommands2.onNext(outCommands1.values().get(0));
        counter2.increment(5);

        // then
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();
        assertThat(outCommands2.values(), contains(
                new UpdateCommandMatcher(CRDT_ID, HashMap.ofEntries(
                        Tuple.of(NODE_ID_1, 3L),
                        Tuple.of(NODE_ID_2, 5L)
                ))
        ));
    }


    private class UpdateCommandMatcher extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final Map<String, Long> entries;

        private UpdateCommandMatcher(String crdtId, Map<String, Long> entries) {
            super(String.format("UpdateCommandMatcher[crdtId=%s,entries=%s]", crdtId, entries));
            this.crdtId = crdtId;
            this.entries = entries;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof GCounter.UpdateCommand) {
                final GCounter.UpdateCommand command = (GCounter.UpdateCommand) o;
                return Objects.equals(command.getCrdtId(), crdtId)
                        && Objects.equals(command.getEntries(), entries);
            }
            return false;
        }
    }
}