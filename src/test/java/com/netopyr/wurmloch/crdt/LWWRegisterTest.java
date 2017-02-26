package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import org.hamcrest.CustomMatcher;
import org.reactivestreams.Processor;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LWWRegisterTest {

    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String NODE_ID_3 = "N_3";
    private static final String CRDT_ID = "ID_1";

    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new LWWRegister<>(null, CRDT_ID);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new LWWRegister<String>(NODE_ID_1, null);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void subscribingToNullPublisherShouldThrow() {
        final LWWRegister<String> register = new LWWRegister<>(NODE_ID_1, CRDT_ID);
        register.subscribeTo(null);
    }


    @Test (expectedExceptions = NullPointerException.class)
    public void subscribingNullSubscriberShouldThrow() {
        final LWWRegister<String> register = new LWWRegister<>(NODE_ID_1, CRDT_ID);
        register.subscribe(null);
    }


    @Test
    public void itShouldInitializeWithWaitingInCommands() {
        // given
        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID);
        register1.set("Hello World");
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID);

        // when
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);

        // then
        assertThat(register2.get(), is("Hello World"));
    }


    @Test
    public void itShouldGetAndSetValues() {
        // given
        final LWWRegister<String> register = new LWWRegister<>(NODE_ID_1, CRDT_ID);

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
        final LWWRegister<String> register = new LWWRegister<>(NODE_ID_1, CRDT_ID);
        register.subscribe(subscriber);

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(CRDT_ID, "Hello World")
        ));

        // when
        register.set("Hello World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(CRDT_ID, "Hello World")
        ));

        // when
        register.set("Goodbye World");

        // then
        subscriber.assertNotComplete();
        subscriber.assertNoErrors();
        assertThat(subscriber.values(), contains(
                new SetCommandMatcher<>(CRDT_ID, "Hello World"),
                new SetCommandMatcher<>(CRDT_ID, "Goodbye World")
        ));
    }


    @Test
    public void itShouldAcceptNewerValueFromReceivedCommands() {
        // given
        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID);
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID);
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);

        // when
        register1.set("Hello World");

        // then
        assertThat(register2.get(), is("Hello World"));

        // when
        register2.set("Goodbye World");

        // then
        assertThat(register1.get(), is("Goodbye World"));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldIgnoreOlderValueFromReceivedCommands() {
        // given
        final TestSubscriber<LWWRegister.SetCommand<String>> outCommands1 = TestSubscriber.create();
        final TestSubscriber<LWWRegister.SetCommand<String>> outCommands2 = TestSubscriber.create();
        final Processor<LWWRegister.SetCommand<String>, LWWRegister.SetCommand<String>> inCommands3 = ReplayProcessor.create();
        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID);
        register1.subscribe(outCommands1);
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID);
        register2.subscribe(outCommands2);
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);
        final LWWRegister<String> register3 = new LWWRegister<>(NODE_ID_3, CRDT_ID);
        register3.subscribeTo(inCommands3);

        // when
        register1.set("Hello World");
        register2.set("Goodbye World");
        final LWWRegister.SetCommand<String> oldCommand = outCommands1.values().get(0);
        final LWWRegister.SetCommand<String> newCommand = outCommands2.values().get(1);
        inCommands3.onNext(newCommand);
        inCommands3.onNext(oldCommand);

        // then
        assertThat(register3.get(), is("Goodbye World"));
    }


    @Test
    public void itShouldChooseLargerReplicaIdIfCommandsAreConcurrent() {
        // given

        final LWWRegister<String> register1 = new LWWRegister<>(NODE_ID_1, CRDT_ID);
        final LWWRegister<String> register2 = new LWWRegister<>(NODE_ID_2, CRDT_ID);

        // when
        register1.set("Hello World");
        register2.set("Goodbye World");
        register1.subscribeTo(register2);
        register2.subscribeTo(register1);

        // then
        assertThat(register1.get(), is("Goodbye World"));
        assertThat(register2.get(), is("Goodbye World"));
    }

    private static class SetCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final T value;

        private SetCommandMatcher(String crdtId, T value) {
            super(String.format("SetLWWCommandMatcher[crdtId=%s,value=%s]", crdtId, value));
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
