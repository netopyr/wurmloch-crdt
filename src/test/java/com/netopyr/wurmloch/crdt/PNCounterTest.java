package com.netopyr.wurmloch.crdt;

import io.reactivex.subscribers.TestSubscriber;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.hamcrest.CustomMatcher;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class PNCounterTest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String CRDT_ID = "ID_1";


    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new PNCounter(null, CRDT_ID);
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new PNCounter(NODE_ID_1, null);
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void subscribingToNullPublisherShouldThrow() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);
        counter.subscribeTo(null);
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void subscribingNullSubscriberShouldThrow() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);
        counter.subscribe(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByNullShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);
        counter.increment(0L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByNegativeValueShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);
        counter.increment(-1L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void decrementingByNullShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);
        counter.decrement(0L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void decrementingByANegativeValueShouldFail() {
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);
        counter.decrement(-1L);
    }


    @Test
    public void itShouldInitializeWithWaitingInCommands() {
        // given
        final PNCounter counter1 = new PNCounter(NODE_ID_1, CRDT_ID);
        counter1.increment(42L);
        final PNCounter counter2 = new PNCounter(NODE_ID_2, CRDT_ID);

        // when
        counter1.subscribeTo(counter2);
        counter2.subscribeTo(counter1);

        // then
        assertThat(counter2.get(), is(42L));
    }


    @Test
    public void itShouldGetAndIncrementAndDecrementTheValue() {
        // given
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);

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


    @Test
    public void itShouldSendCommandsOnUpdates() {
        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final PNCounter counter = new PNCounter(NODE_ID_1, CRDT_ID);
        counter.subscribe(subscriber);

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
        final PNCounter counter1 = new PNCounter(NODE_ID_1, CRDT_ID);
        final PNCounter counter2 = new PNCounter(NODE_ID_2, CRDT_ID);
        counter1.subscribeTo(counter2);
        counter2.subscribeTo(counter1);

        // when
        counter1.increment();

        // then
        assertThat(counter2.get(), is(1L));

        // when
        counter2.increment();

        // then
        assertThat(counter1.get(), is(2L));
        assertThat(counter2.get(), is(2L));

        // when
        counter1.decrement();

        // then
        assertThat(counter1.get(), is(1L));
        assertThat(counter2.get(), is(1L));

        // when
        counter2.decrement();

        // then
        assertThat(counter1.get(), is(0L));
        assertThat(counter2.get(), is(0L));
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