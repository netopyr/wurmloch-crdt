package com.netopyr.megastore.crdt.lwwregister;

import com.netopyr.megastore.crdt.CrdtCommand;
import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class LWWRegisterTest {

    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new LWWRegister<>(null, "ID_1", mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new LWWRegister<String>("R_1", null, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullPublisherShouldThrow() {
        new LWWRegister<String>("R_1", "ID_1", null, mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullSubscriberShouldThrow() {
        new LWWRegister<String>("R_1", "ID_1", mock(Publisher.class), null);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldGetAndSetValues() {
        // given
        final LWWRegister<String> register = new LWWRegister<>("R_1", "ID_1", mock(Publisher.class), mock(Subscriber.class));

        // when
        final String v0 = register.get();

        // then
        assertThat(v0, is(nullValue()));

        // when
        register.set("Hello World");
        final String v1 = register.get();

        // then
        assertThat(v1, is("Hello World"));

        // when
        register.set("Goodbye World");
        final String v2 = register.get();

        // then
        assertThat(v2, is("Goodbye World"));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldSendCommandsOnUpdates() {
        final String REPLICA_ID = "R_1";
        final String CRDT_ID = "ID_1";

        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final LWWRegister<String> register = new LWWRegister<>(REPLICA_ID, CRDT_ID, mock(Publisher.class), subscriber);

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetLWWCommandMatcher<>(REPLICA_ID, CRDT_ID, "Hello World")
        ));

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetLWWCommandMatcher<>(REPLICA_ID, CRDT_ID, "Hello World")
        ));

        // when
        register.set("Goodbye World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetLWWCommandMatcher<>(REPLICA_ID, CRDT_ID, "Hello World"),
                new SetLWWCommandMatcher<>(REPLICA_ID, CRDT_ID, "Goodbye World")
        ));
    }


    @Test
    public void itShouldAcceptNewerValueFromReceivedCommands() {
        final String REPLICA_ID_1 = "R_1";
        final String REPLICA_ID_2 = "R_2";
        final String CRDT_ID = "ID_1";

        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final LWWRegister<String> register1 = new LWWRegister<>(REPLICA_ID_1, CRDT_ID, inCommands1, outCommands1);
        final LWWRegister<String> register2 = new LWWRegister<>(REPLICA_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        register1.set("Hello World");

        // then
        assertThat(outCommands1.valueCount(), is(1));
        outCommands1.assertNotComplete();
        outCommands1.assertNoErrors();

        // when
        inCommands2.onNext(outCommands1.values().get(0));

        // then
        assertThat(register2.get(), is("Hello World"));
        outCommands2.assertNoValues();
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();

        // when
        register2.set("Goodbye World");

        // then
        assertThat(outCommands2.valueCount(), is(1));
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();

        // when
        inCommands1.onNext(outCommands2.values().get(0));

        // then
        assertThat(register1.get(), is("Goodbye World"));
        assertThat(outCommands1.valueCount(), is(1));
        outCommands1.assertNotComplete();
        outCommands1.assertNoErrors();
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldIgnoreOlderValueFromReceivedCommands() {
        final String REPLICA_ID_1 = "R_1";
        final String REPLICA_ID_2 = "R_2";
        final String REPLICA_ID_3 = "R_3";
        final String CRDT_ID = "ID_1";

        // given
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands3 = ReplayProcessor.create();
        final LWWRegister<String> register1 = new LWWRegister<>(REPLICA_ID_1, CRDT_ID, mock(Publisher.class), outCommands1);
        final LWWRegister<String> register2 = new LWWRegister<>(REPLICA_ID_2, CRDT_ID, inCommands2, outCommands2);
        final LWWRegister<String> register3 = new LWWRegister<>(REPLICA_ID_3, CRDT_ID, inCommands3, mock(Subscriber.class));

        // when
        register1.set("Hello World");
        final CrdtCommand oldCommand = outCommands1.values().get(0);
        inCommands2.onNext(oldCommand);
        register2.set("Goodbye World");
        final CrdtCommand newCommand = outCommands2.values().get(0);
        inCommands3.onNext(newCommand);
        inCommands3.onNext(oldCommand);

        // then
        assertThat(register3.get(), is("Goodbye World"));
    }


    @Test
    public void itShouldChooseSmallerReplicaIdIfCommandsAreConcurrent() {
        final String REPLICA_ID_1 = "R_1";
        final String REPLICA_ID_2 = "R_2";
        final String CRDT_ID = "ID_1";

        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final LWWRegister<String> register1 = new LWWRegister<>(REPLICA_ID_1, CRDT_ID, inCommands1, outCommands1);
        final LWWRegister<String> register2 = new LWWRegister<>(REPLICA_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        register1.set("Hello World");
        register2.set("Goodbye World");
        inCommands1.onNext(outCommands2.values().get(0));
        inCommands2.onNext(outCommands1.values().get(0));

        // then
        assertThat(register1.get(), is("Hello World"));
        assertThat(register2.get(), is("Hello World"));
    }
}
