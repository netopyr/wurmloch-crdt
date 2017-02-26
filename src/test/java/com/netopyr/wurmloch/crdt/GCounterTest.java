package com.netopyr.wurmloch.crdt;

import com.netopyr.wurmloch.crdt.GCounter.UpdateCommand;
import io.reactivex.subscribers.TestSubscriber;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.hamcrest.CustomMatcher;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class GCounterTest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String CRDT_ID = "ID_1";


    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new GCounter(null, CRDT_ID);
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new GCounter(NODE_ID_1, null);
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void subscribingToNullPublisherShouldThrow() {
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID);
        counter.subscribeTo(null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void subscribingNullSubscriberShouldThrow() {
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID);
        counter.subscribe(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByNullShouldFail() {
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID);
        counter.increment(0L);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void incrementingByANegativeValueShouldFail() {
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID);
        counter.increment(-1L);
    }


    @Test
    public void itShouldInitializeWithWaitingInCommands() {
        // given
        final GCounter counter1 = new GCounter(NODE_ID_1, CRDT_ID);
        counter1.increment(42L);

        // when
        final GCounter counter2 = new GCounter(NODE_ID_2, CRDT_ID);
        counter1.subscribeTo(counter2);
        counter2.subscribeTo(counter1);

        // then
        assertThat(counter2.get(), is(42L));
    }


    @Test
    public void itShouldGetAndIncrementTheValue() {
        // given
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID);

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


    @Test
    public void itShouldSendCommandsOnUpdates() {
        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final GCounter counter = new GCounter(NODE_ID_1, CRDT_ID);
        counter.subscribe(subscriber);

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
        final TestSubscriber<UpdateCommand> outCommands1 = TestSubscriber.create();
        final TestSubscriber<UpdateCommand> outCommands2 = TestSubscriber.create();
        final GCounter counter1 = new GCounter(NODE_ID_1, CRDT_ID);
        counter1.subscribe(outCommands1);
        final GCounter counter2 = new GCounter(NODE_ID_2, CRDT_ID);
        counter2.subscribe(outCommands2);
        counter1.subscribeTo(counter2);
        counter2.subscribeTo(counter1);

        // when
        counter1.increment();

        // then
        assertThat(counter1.get(), is(1L));
        assertThat(counter2.get(), is(1L));
        assertThat(outCommands1.valueCount(), is(1));
        outCommands1.assertNotComplete();
        outCommands1.assertNoErrors();
        assertThat(outCommands2.valueCount(), is(1));
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();

        // when
        counter2.increment(2L);

        // then
        assertThat(counter1.get(), is(3L));
        assertThat(counter2.get(), is(3L));
        assertThat(outCommands1.valueCount(), is(2));
        outCommands1.assertNotComplete();
        outCommands1.assertNoErrors();
        assertThat(outCommands2.valueCount(), is(2));
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();
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
            if (o instanceof UpdateCommand) {
                final UpdateCommand command = (UpdateCommand) o;
                return Objects.equals(command.getCrdtId(), crdtId)
                        && Objects.equals(command.getEntries(), entries);
            }
            return false;
        }
    }
}