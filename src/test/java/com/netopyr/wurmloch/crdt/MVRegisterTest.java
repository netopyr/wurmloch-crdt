package com.netopyr.wurmloch.crdt;

import io.reactivex.processors.ReplayProcessor;
import io.reactivex.subscribers.TestSubscriber;
import javaslang.collection.Array;
import org.hamcrest.CustomMatcher;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MVRegisterTest {

    @SuppressWarnings("unchecked")
    @Test(expectedExceptions = NullPointerException.class)
    public void constructorWithNullReplicaShouldThrow() {
        new MVRegister<>(null, "ID_1", mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullIdShouldThrow() {
        new MVRegister<String>("R_1", null, mock(Publisher.class), mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullPublisherShouldThrow() {
        new MVRegister<String>("R_1", "ID_1", null, mock(Subscriber.class));
    }


    @SuppressWarnings("unchecked")
    @Test (expectedExceptions = NullPointerException.class)
    public void constructorWithNullSubscriberShouldThrow() {
        new MVRegister<String>("R_1", "ID_1", mock(Publisher.class), null);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldGetAndSetValues() {
        final String NODE_ID = "N_1";
        final String CRDT_ID = "ID_1";

        // given
        final MVRegister<String> register = new MVRegister<>(NODE_ID, CRDT_ID, mock(Publisher.class), mock(Subscriber.class));

        // when
        final Array<String> v0 = register.get();

        // then
        assertThat(v0, is(emptyIterable()));

        // when
        register.set("Hello World");
        final Array<String> v1 = register.get();

        // then
        assertThat(v1, contains("Hello World"));

        // when
        register.set("Goodbye World");
        final Array<String> v2 = register.get();

        // then
        assertThat(v2, contains("Goodbye World"));
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldSendCommandsOnUpdates() {
        final String NODE_ID = "N_1";
        final String CRDT_ID = "ID_1";

        // given
        final TestSubscriber<CrdtCommand> subscriber = TestSubscriber.create();
        final MVRegister<String> register = new MVRegister<>(NODE_ID, CRDT_ID, mock(Publisher.class), subscriber);

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
        assertThat(subscriber.values(), containsInAnyOrder(
                new SetCommandMatcher<>(CRDT_ID, "Hello World"),
                new SetCommandMatcher<>(CRDT_ID, "Goodbye World")
        ));
    }


    @Test
    public void itShouldAcceptNewerValueFromReceivedCommands() {
        final String NODE_ID_1 = "N_1";
        final String NODE_ID_2 = "N_2";
        final String CRDT_ID = "ID_1";

        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        register1.set("Hello World");

        // then
        assertThat(outCommands1.valueCount(), is(1));
        outCommands1.assertNotComplete();
        outCommands1.assertNoErrors();

        // when
        inCommands2.onNext(outCommands1.values().get(0));

        // then
        assertThat(register2.get(), contains("Hello World"));
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
        assertThat(register1.get(), contains("Goodbye World"));
        assertThat(outCommands1.valueCount(), is(1));
        outCommands1.assertNotComplete();
        outCommands1.assertNoErrors();
    }


    @SuppressWarnings("unchecked")
    @Test
    public void itShouldIgnoreOlderValueFromReceivedCommands() {
        final String NODE_ID_1 = "N_1";
        final String NODE_ID_2 = "N_2";
        final String NODE_ID_3 = "R_3";
        final String CRDT_ID = "ID_1";

        // given
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands3 = ReplayProcessor.create();
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID, mock(Publisher.class), outCommands1);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);
        final MVRegister<String> register3 = new MVRegister<>(NODE_ID_3, CRDT_ID, inCommands3, mock(Subscriber.class));

        // when
        register1.set("Hello World");
        final CrdtCommand oldCommand = outCommands1.values().get(0);
        inCommands2.onNext(oldCommand);
        register2.set("Goodbye World");
        final CrdtCommand newCommand = outCommands2.values().get(0);
        inCommands3.onNext(newCommand);
        inCommands3.onNext(oldCommand);

        // then
        assertThat(register3.get(), contains("Goodbye World"));
    }


    @Test
    public void itShouldAddOtherValueIfCommandsAreConcurrent() {
        final String NODE_ID_1 = "N_1";
        final String NODE_ID_2 = "N_2";
        final String CRDT_ID = "ID_1";

        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        // when
        register1.set("Hello World");
        register2.set("Goodbye World");
        inCommands1.onNext(outCommands2.values().get(0));
        inCommands2.onNext(outCommands1.values().get(0));

        // then
        assertThat(register1.get(), containsInAnyOrder("Hello World", "Goodbye World"));
        assertThat(register2.get(), containsInAnyOrder("Hello World", "Goodbye World"));
    }


    @Test
    public void itShouldOverwriteConcurrentValues() {
        final String NODE_ID_1 = "N_1";
        final String NODE_ID_2 = "N_2";
        final String CRDT_ID = "ID_1";

        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        register1.set("Hello World");
        register2.set("Goodbye World");
        inCommands1.onNext(outCommands2.values().get(0));
        inCommands2.onNext(outCommands1.values().get(0));

        // when
        register1.set("42");

        // then
        assertThat(register1.get(), containsInAnyOrder("42"));
        assertThat(register2.get(), containsInAnyOrder("Hello World", "Goodbye World"));

        // when
        inCommands2.onNext(outCommands1.values().get(1));

        // then
        assertThat(register1.get(), containsInAnyOrder("42"));
        assertThat(register2.get(), containsInAnyOrder("42"));
    }


    @Test
    public void itShouldOverwriteOnlyPartialCommandsFromReceivedCommand() {
        final String NODE_ID_1 = "N_1";
        final String NODE_ID_2 = "N_2";
        final String CRDT_ID = "ID_1";

        // given
        final Processor<CrdtCommand, CrdtCommand> inCommands1 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands1 = TestSubscriber.create();
        final Processor<CrdtCommand, CrdtCommand> inCommands2 = ReplayProcessor.create();
        final TestSubscriber<CrdtCommand> outCommands2 = TestSubscriber.create();
        final MVRegister<String> register1 = new MVRegister<>(NODE_ID_1, CRDT_ID, inCommands1, outCommands1);
        final MVRegister<String> register2 = new MVRegister<>(NODE_ID_2, CRDT_ID, inCommands2, outCommands2);

        register1.set("Hello World");
        register2.set("Goodbye World");
        inCommands2.onNext(outCommands1.values().get(0));

        // when
        register1.set("42");

        // then
        assertThat(register1.get(), containsInAnyOrder("42"));
        assertThat(register2.get(), containsInAnyOrder("Hello World", "Goodbye World"));

        // when
        inCommands2.onNext(outCommands1.values().get(1));

        // then
        assertThat(register1.get(), containsInAnyOrder("42"));
        assertThat(register2.get(), containsInAnyOrder("42", "Goodbye World"));
    }

    private static class SetCommandMatcher<T> extends CustomMatcher<CrdtCommand> {

        private final String crdtId;
        private final T value;

        private SetCommandMatcher(String crdtId, T value) {
            super(String.format("SetMVCommandMatcher[crdtId=%s,value=%s]", crdtId, value));
            this.crdtId = crdtId;
            this.value = value;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof MVRegister.SetCommand) {
                final MVRegister.SetCommand command = (MVRegister.SetCommand) o;
                return Objects.equals(command.getCrdtId(), crdtId)
                        && Objects.equals(command.getEntry().getValue(), value)
                        && command.getEntry().getClock() != null;
            }
            return false;
        }
    }
}
