package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import org.reactivestreams.Processor;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class RGATest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String CRDT_ID = "ID_1";


    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new RGA<>(null, CRDT_ID);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new RGA<String>(NODE_ID_1, null);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void subscribingToNullPublisherShouldThrow() {
        final RGA<String> rga = new RGA<>(NODE_ID_1, CRDT_ID);
        rga.subscribeTo(null);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void subscribingNullSubscriberShouldThrow() {
        final RGA<String> rga = new RGA<>(NODE_ID_1, CRDT_ID);
        rga.subscribe(null);
    }


    @Test
    public void itShouldSetupEmptyList() {
        // when
        final RGA<String> rga = new RGA<>(NODE_ID_1, CRDT_ID);

        // then
        assertThat(rga, is(empty()));
    }


    @Test
    public void itShouldAddElements() {
        // given
        final RGA<String> rga1 = new RGA<>(NODE_ID_1, CRDT_ID);
        final RGA<String> rga2 = new RGA<>(NODE_ID_2, CRDT_ID);
        rga1.subscribeTo(rga2);
        rga2.subscribeTo(rga1);

        // when
        rga1.add(0, "A");

        // then
        assertThat(rga1, contains("A"));
        assertThat(rga2, contains("A"));

        // when
        rga2.add(0, "B2");

        // then
        assertThat(rga1, contains("B2", "A"));
        assertThat(rga2, contains("B2", "A"));

        rga1.add(0, "B1");

        // then
        assertThat(rga1, contains("B1", "B2", "A"));
        assertThat(rga2, contains("B1", "B2", "A"));

        // when
        rga2.add(1, "C2");

        // then
        assertThat(rga1, contains("B1", "C2", "B2", "A"));
        assertThat(rga2, contains("B1", "C2", "B2", "A"));

        // when
        rga1.add(1, "C1");

        // then
        assertThat(rga1, contains("B1", "C1", "C2", "B2", "A"));
        assertThat(rga2, contains("B1", "C1", "C2", "B2", "A"));

        // when
        rga2.add(5, "D2");

        // then
        assertThat(rga1, contains("B1", "C1", "C2", "B2", "A", "D2"));
        assertThat(rga2, contains("B1", "C1", "C2", "B2", "A", "D2"));

        // when
        rga1.add(6, "D1");

        // then
        assertThat(rga1, contains("B1", "C1", "C2", "B2", "A", "D2", "D1"));
        assertThat(rga2, contains("B1", "C1", "C2", "B2", "A", "D2", "D1"));
    }


    @Test
    public void itShouldRemoveElements() {
        // given
        final RGA<String> rga1 = new RGA<>(NODE_ID_1, CRDT_ID);
        final RGA<String> rga2 = new RGA<>(NODE_ID_2, CRDT_ID);
        rga1.subscribeTo(rga2);
        rga2.subscribeTo(rga1);
        rga1.addAll(Arrays.asList("B2", "C2", "C1", "B1", "A", "D1", "D2"));

        // when
        rga2.remove(6);

        // then
        assertThat(rga1, contains("B2", "C2", "C1", "B1", "A", "D1"));
        assertThat(rga2, contains("B2", "C2", "C1", "B1", "A", "D1"));

        // when
        rga1.remove(5);

        // then
        assertThat(rga1, contains("B2", "C2", "C1", "B1", "A"));
        assertThat(rga2, contains("B2", "C2", "C1", "B1", "A"));

        // when
        rga2.remove(1);

        // then
        assertThat(rga1, contains("B2", "C1", "B1", "A"));
        assertThat(rga2, contains("B2", "C1", "B1", "A"));

        // when
        rga1.remove(1);

        // then
        assertThat(rga1, contains("B2", "B1", "A"));
        assertThat(rga2, contains("B2", "B1", "A"));

        // when
        rga2.remove(0);

        // then
        assertThat(rga1, contains("B1", "A"));
        assertThat(rga2, contains("B1", "A"));

        // when
        rga1.remove(0);

        // then
        assertThat(rga1, contains("A"));
        assertThat(rga2, contains("A"));


        // when
        rga2.remove(0);

        // then
        assertThat(rga1, is(empty()));
        assertThat(rga2, is(empty()));
    }


    @Test
    public void itShouldAddElementsConcurrently() {
        int i1 = 0;
        int i2 = 0;

        // given
        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands1 = TestSubscriber.create();
        final RGA<String> rga1 = new RGA<>(NODE_ID_1, CRDT_ID);
        rga1.subscribeTo(inCommands1);
        rga1.subscribe(outCommands1);

        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands2 = TestSubscriber.create();
        final RGA<String> rga2 = new RGA<>(NODE_ID_2, CRDT_ID);
        rga2.subscribeTo(inCommands2);
        rga2.subscribe(outCommands2);

        // when
        rga1.add(0, "A1");
        rga2.add(0, "A2");
        inCommands2.onNext(outCommands1.values().get(i1));
        inCommands1.onNext(outCommands2.values().get(i2));

        // then
        assertThat(rga1, contains("A2", "A1"));
        assertThat(rga2, contains("A2", "A1"));

        // when
        rga1.add(0, "B1");
        rga2.add(0, "B2");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("B2", "B1", "A2", "A1"));
        assertThat(rga2, contains("B2", "B1", "A2", "A1"));

        // when
        rga1.add(1, "C1");
        rga2.add(1, "C2");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("B2", "C2", "C1", "B1", "A2", "A1"));
        assertThat(rga2, contains("B2", "C2", "C1", "B1", "A2", "A1"));

        // when
        rga1.add(6, "D1");
        rga2.add(6, "D2");
        inCommands2.onNext(outCommands1.values().get(i1 + 2));
        inCommands1.onNext(outCommands2.values().get(i2 + 2));

        // then
        assertThat(rga1, contains("B2", "C2", "C1", "B1", "A2", "A1", "D2", "D1"));
        assertThat(rga2, contains("B2", "C2", "C1", "B1", "A2", "A1", "D2", "D1"));
    }


    @Test
    public void itShouldRemoveElementsConcurrently() {
        int i1 = 0;

        // given
        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands1 = TestSubscriber.create();
        final RGA<String> rga1 = new RGA<>(NODE_ID_1, CRDT_ID);
        rga1.subscribeTo(inCommands1);
        rga1.subscribe(outCommands1);

        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands2 = TestSubscriber.create();
        final RGA<String> rga2 = new RGA<>(NODE_ID_2, CRDT_ID);
        rga2.subscribeTo(inCommands2);
        rga2.subscribe(outCommands2);

        rga1.addAll(Arrays.asList("B0", "C0", "C1", "C2", "B1", "B2", "A1", "A2", "D1", "D2", "D0"));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        int i2 = i1;

        // when
        rga1.remove(10);
        rga2.remove(10);
        inCommands2.onNext(outCommands1.values().get(i1));
        inCommands1.onNext(outCommands2.values().get(i2));

        // then
        assertThat(rga1, contains("B0", "C0", "C1", "C2", "B1", "B2", "A1", "A2", "D1", "D2"));
        assertThat(rga2, contains("B0", "C0", "C1", "C2", "B1", "B2", "A1", "A2", "D1", "D2"));

        // when
        rga1.remove(9);
        rga2.remove(8);
        inCommands2.onNext(outCommands1.values().get(++i1));
        inCommands1.onNext(outCommands2.values().get(++i2));

        // then
        assertThat(rga1, contains("B0", "C0", "C1", "C2", "B1", "B2", "A1", "A2"));
        assertThat(rga2, contains("B0", "C0", "C1", "C2", "B1", "B2", "A1", "A2"));

        // when
        rga1.remove(1);
        rga2.remove(1);
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("B0", "C1", "C2", "B1", "B2", "A1", "A2"));
        assertThat(rga2, contains("B0", "C1", "C2", "B1", "B2", "A1", "A2"));

        // when
        rga1.remove(1);
        rga2.remove(2);
        inCommands2.onNext(outCommands1.values().get(++i1));
        inCommands1.onNext(outCommands2.values().get(++i2));

        // then
        assertThat(rga1, contains("B0", "B1", "B2", "A1", "A2"));
        assertThat(rga2, contains("B0", "B1", "B2", "A1", "A2"));

        // when
        rga1.remove(0);
        rga2.remove(0);
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("B1", "B2", "A1", "A2"));
        assertThat(rga2, contains("B1", "B2", "A1", "A2"));

        // when
        rga1.remove(0);
        rga2.remove(1);
        inCommands2.onNext(outCommands1.values().get(++i1));
        inCommands1.onNext(outCommands2.values().get(++i2));

        // then
        assertThat(rga1, contains("A1", "A2"));
        assertThat(rga2, contains("A1", "A2"));

        // when
        rga1.remove(0);
        rga2.remove(1);
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, is(empty()));
        assertThat(rga2, is(empty()));

        // when
        rga1.add("A0");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        rga1.remove(0);
        rga2.remove(0);
        inCommands2.onNext(outCommands1.values().get(i1 + 1));
        inCommands1.onNext(outCommands2.values().get(i2 + 2));

        // then
        assertThat(rga1, is(empty()));
        assertThat(rga2, is(empty()));
    }


    @Test
    public void itShouldAddAndRemoveSingleElementConcurrently() {
        int i1 = 0;

        // given
        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands1 = TestSubscriber.create();
        final RGA<String> rga1 = new RGA<>(NODE_ID_1, CRDT_ID);
        rga1.subscribeTo(inCommands1);
        rga1.subscribe(outCommands1);

        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands2 = TestSubscriber.create();
        final RGA<String> rga2 = new RGA<>(NODE_ID_2, CRDT_ID);
        rga2.subscribeTo(inCommands2);
        rga2.subscribe(outCommands2);

        rga1.add("A");
        inCommands2.onNext(outCommands1.values().get(i1));
        int i2 = i1;

        // when
        rga1.remove(0);
        rga2.add(0, "B");
        inCommands2.onNext(outCommands1.values().get(++i1));
        inCommands1.onNext(outCommands2.values().get(++i2));

        // then
        assertThat(rga1, contains("B"));
        assertThat(rga2, contains("B"));

        // when
        rga1.remove(0);
        rga2.add(1, "C");
        inCommands2.onNext(outCommands1.values().get(i1 + 2));
        inCommands1.onNext(outCommands2.values().get(i2 + 2));

        // then
        assertThat(rga1, contains("C"));
        assertThat(rga2, contains("C"));
    }


    @Test
    public void itShouldAddAndRemoveElementsConcurrently() {
        int i1 = 0;

        // given
        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands1 = TestSubscriber.create();
        final RGA<String> rga1 = new RGA<>(NODE_ID_1, CRDT_ID);
        rga1.subscribeTo(inCommands1);
        rga1.subscribe(outCommands1);

        final Processor<RGA.RGACommand<String>, RGA.RGACommand<String>> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<RGA.RGACommand<String>> outCommands2 = TestSubscriber.create();
        final RGA<String> rga2 = new RGA<>(NODE_ID_2, CRDT_ID);
        rga2.subscribeTo(inCommands2);
        rga2.subscribe(outCommands2);

        rga1.addAll(Arrays.asList("A", "B", "C", "D", "E"));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        inCommands2.onNext(outCommands1.values().get(i1++));
        int i2 = i1;

        // when
        rga1.remove(0);
        rga2.add(0, "A1");
        inCommands2.onNext(outCommands1.values().get(i1));
        inCommands1.onNext(outCommands2.values().get(i2));

        // then
        assertThat(rga1, contains("A1", "B", "C", "D", "E"));
        assertThat(rga2, contains("A1", "B", "C", "D", "E"));

        // when
        rga1.remove(0);
        rga2.add(1, "A2");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("A2", "B", "C", "D", "E"));
        assertThat(rga2, contains("A2", "B", "C", "D", "E"));

        // when
        rga1.remove(1);
        rga2.add(0, "A3");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("A3", "A2", "C", "D", "E"));
        assertThat(rga2, contains("A3", "A2", "C", "D", "E"));

        // when
        rga1.remove(2);
        rga2.add(1, "B1");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("A3", "B1", "A2", "D", "E"));
        assertThat(rga2, contains("A3", "B1", "A2", "D", "E"));

        // when
        rga1.remove(2);
        rga2.add(2, "B2");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("A3", "B1", "B2", "D", "E"));
        assertThat(rga2, contains("A3", "B1", "B2", "D", "E"));

        // when
        rga1.remove(2);
        rga2.add(3, "B3");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("A3", "B1", "B3", "D", "E"));
        assertThat(rga2, contains("A3", "B1", "B3", "D", "E"));

        // when
        rga1.remove(4);
        rga2.add(4, "C1");
        inCommands2.onNext(outCommands1.values().get(i1+=2));
        inCommands1.onNext(outCommands2.values().get(i2+=2));

        // then
        assertThat(rga1, contains("A3", "B1", "B3", "D", "C1"));
        assertThat(rga2, contains("A3", "B1", "B3", "D", "C1"));

        // when
        rga1.remove(4);
        rga2.add(5, "C2");
        inCommands2.onNext(outCommands1.values().get(i1 + 2));
        inCommands1.onNext(outCommands2.values().get(i2 + 2));

        // then
        assertThat(rga1, contains("A3", "B1", "B3", "D", "C2"));
        assertThat(rga2, contains("A3", "B1", "B3", "D", "C2"));
    }
}
