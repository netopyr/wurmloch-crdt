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

public class PNCounterTest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String CRDT_ID = "ID_1";


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new PNCounter(null, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new PNCounter(NODE_ID_1, null, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullPublisherShouldThrow() {
        new PNCounter(NODE_ID_1, CRDT_ID, null, mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullSubscriberShouldThrow() {
        new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), null);
    }

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByNullShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
        counter.increment(0L);
    }

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByANegativeValueShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
        counter.increment(-1L);
    }

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void decrementingByNullShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
        counter.decrement(0L);
    }

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void decrementingByANegativeValueShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
        counter.decrement(-1L);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldInitializeWithWaitingInCommands() {
        // given
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final PNCounter counter1 = new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), outCommands1);
        counter1.increment(42L);

        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        inCommands2.onNext(outCommands1.values().get(0));

        // when
        final PNCounter counter2 = new PNCounter(NODE_ID_2, CRDT_ID, inCommands2, mock(Subscriber.class));

        // then
        assertThat(counter2.get(), is(42L));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldGetAndIncrementAndDecrementTheValue() {
        // given
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));

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
        counter.decrement(3L);
        final Long v2 = counter.get();

        // then
        assertThat(v2, is(-2L));

        // when
        counter.increment(5L);
        final Long v3 = counter.get();

        // then
        assertThat(v3, is(3L));

        // when
        counter.decrement();
        final Long v4 = counter.get();

        // then
        assertThat(v4, is(2L));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldSendCommandsOnUpdates() {
        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID, mock(Publisher.class), subscriber);

        // when
        counter.increment();
        counter.decrement();
        counter.increment(3L);
        counter.decrement(5L);

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new UpdateCommandMatcher(CRDT_ID, HashMap.of(NODE_ID_1, 1L), HashMap.empty()),
                new UpdateCommandMatcher(CRDT_ID, HashMap.of(NODE_ID_1, 1L), HashMap.of(NODE_ID_1, 1L)),
                new UpdateCommandMatcher(CRDT_ID, HashMap.of(NODE_ID_1, 4L), HashMap.of(NODE_ID_1, 1L)),
                new UpdateCommandMatcher(CRDT_ID, HashMap.of(NODE_ID_1, 4L), HashMap.of(NODE_ID_1, 6L))
        ));
    }


    @Test
    public void itShouldAcceptUpdatesFromReceivedCommands() {
        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final PNCounter counter1 = new PNCounter(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final PNCounter counter2 = new PNCounter(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

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

        // when
        counter1.decrement();
        inCommands2.onNext(outCommands1.values().get(1));

        // then
        assertThat(counter1.get(), is(1L));
        assertThat(counter2.get(), is(1L));
        assertThat(outCommands2.valueCount(), is(1));
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();

        // when
        counter2.decrement();
        inCommands1.onNext(outCommands2.values().get(1));

        // then
        assertThat(counter1.get(), is(0L));
        assertThat(counter2.get(), is(0L));
        assertThat(outCommands1.valueCount(), is(2));
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
        final PNCounter counter1 = new PNCounter(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final PNCounter counter2 = new PNCounter(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        counter1.increment(3);
        inCommands2.onNext(outCommands1.values().get(0));
        counter2.increment(5);

        // then
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();
        assertThat(outCommands2.values(), contains(
                new UpdateCommandMatcher(
                        CRDT_ID,
                        HashMap.ofEntries(
                                Tuple.of(NODE_ID_1, 3L),
                                Tuple.of(NODE_ID_2, 5L)
                        ),
                        HashMap.empty()
                )
        ));

        // when
        counter1.decrement(7);
        inCommands2.onNext(outCommands1.values().get(1));
        counter2.decrement(11);

        // then
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();
        assertThat(outCommands2.values(), contains(
                new UpdateCommandMatcher(
                        CRDT_ID,
                        HashMap.ofEntries(
                                Tuple.of(NODE_ID_1, 3L),
                                Tuple.of(NODE_ID_2, 5L)
                        ),
                        HashMap.empty()
                ),
                new UpdateCommandMatcher(
                        CRDT_ID,
                        HashMap.ofEntries(
                                Tuple.of(NODE_ID_1, 3L),
                                Tuple.of(NODE_ID_2, 5L)
                        ),
                        HashMap.ofEntries(
                                Tuple.of(NODE_ID_1, 7L),
                                Tuple.of(NODE_ID_2, 11L)
                        )
                )
        ));
    }


    private class UpdateCommandMatcher extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final Map<String, Long> pEntries;
        private final Map<String, Long> nEntries;

        private UpdateCommandMatcher(String crdtId, Map<String, Long> pEntries, Map<String, Long> nEntries) {
            super(String.format("UpdateCommandMatcher[crdtId=%s,pEntries=%s,nEntries=%s]", crdtId, pEntries, nEntries));
            this.crdtId = crdtId;
            this.pEntries = pEntries;
            this.nEntries = nEntries;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof PNCounter.UpdateCommand) {
                final PNCounter.UpdateCommand command = (PNCounter.UpdateCommand) o;
                return Objects.equals(command.getCrdtId(), crdtId)
                        && Objects.equals(command.getPEntries(), pEntries)
                        && Objects.equals(command.getNEntries(), nEntries);
            }
            return false;
        }
    }
}