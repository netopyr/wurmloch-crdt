package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import org.hamcrest.CustomMatcher;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class LWWRegisterTest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String NODE_ID_3 = "N_3";
    private static final String CRDT_ID = "ID_1";

    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new LWWRegister<>(null, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new LWWRegister<String>(NODE_ID_1, null, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullPublisherShouldThrow() {
        new LWWRegister<String>(NODE_ID_1, CRDT_ID, null, mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullSubscriberShouldThrow() {
        new LWWRegister<String>(NODE_ID_1, CRDT_ID, mock(Publisher.class), null);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldInitializeWithWaitingInCommands() {
        // given
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID, mock(Publisher.class), outCommands1);
        register1.set("Hello World");

        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        inCommands2.onNext(outCommands1.values().get(0));

        // when
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID, inCommands2, mock(Subscriber.class));

        // then
        assertThat(register2.get(), is("Hello World"));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldGetAndSetValues() {
        // given
        final LWWRegister<String> register = new LWWRegister<>(NODE_ID_1, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));

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
        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final LWWRegister<String> register = new LWWRegister<>(NODE_ID_1, CRDT_ID, mock(Publisher.class), subscriber);

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(NODE_ID_1, CRDT_ID, "Hello World")
        ));

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(NODE_ID_1, CRDT_ID, "Hello World")
        ));

        // when
        register.set("Goodbye World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(NODE_ID_1, CRDT_ID, "Hello World"),
                new SetCommandMatcher<>(NODE_ID_1, CRDT_ID, "Goodbye World")
        ));
    }


    @Test
    public void itShouldAcceptNewerValueFromReceivedCommands() {
        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        register1.set("Hello World");
        inCommands2.onNext(outCommands1.values().get(0));

        // then
        assertThat(register2.get(), is("Hello World"));
        outCommands2.assertNoValues();
        outCommands2.assertNotComplete();
        outCommands2.assertNoErrors();

        // when
        register2.set("Goodbye World");
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
        // given
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands3 = ReplayProcessor.create();
        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID, mock(Publisher.class), outCommands1);
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);
        final LWWRegister<String> register3 = new LWWRegister<>(NODE_ID_3, CRDT_ID, inCommands3, mock(Subscriber.class));

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
    public void itShouldChooseLargerReplicaIdIfCommandsAreConcurrent() {
        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        register1.set("Hello World");
        register2.set("Goodbye World");
        inCommands1.onNext(outCommands2.values().get(0));
        inCommands2.onNext(outCommands1.values().get(0));

        // then
        assertThat(register1.get(), is("Goodbye World"));
        assertThat(register2.get(), is("Goodbye World"));
    }

    private static class SetCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

        private final String nodeId;
        private final String crdtId;
        private final T value;

        private SetCommandMatcher(String nodeId, String crdtId, T value) {
            super(String.format("SetLWWCommandMatcher[nodeId=%s,crdtId=%s,value=%s]", nodeId, crdtId, value));
            this.nodeId = nodeId;
            this.crdtId = crdtId;
            this.value = value;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof LWWRegister.SetCommand) {
                final LWWRegister.SetCommand command = (LWWRegister.SetCommand) o;
                return Objects.equals(command.getCrdtId(), crdtId)
                        && Objects.equals(command.getValue(), value)
                        && command.getClock() != null;
            }
            return false;
        }
    }
}
