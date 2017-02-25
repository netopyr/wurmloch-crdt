package com.netopyr.wurmloch.store;

import com.netopyr.wurmloch.crdt.Crdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import io.reactivex.subscribers.TestSubscriber;
import javaslang.control.Option;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LocalCrdtStoreTest {
    
    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    

    @SuppressWarnings("unused")
    private static SimpleCrdt createCrdt(String nodeId, String id) {
        return new SimpleCrdt(id);
    }


    @Test
    public void shouldFindCrdts() {
        // given:
        final LocalCrdtStore store = new LocalCrdtStore();

        // when:
        final Option<? extends Crdt> result1 = store.findCrdt(NODE_ID_1);

        // then:
        assertThat(result1.isDefined(), is(false));

        // when:
        final SimpleCrdt expected = store.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_1);
        final Option<? extends Crdt> result2 = store.findCrdt(NODE_ID_1);

        // then:
        assertThat(result2.get(), is(expected));
    }

    @Test
    public void shouldAddCrdtWhileConnected() {
        // given:
        final LocalCrdtStore store1 = new LocalCrdtStore();
        final LocalCrdtStore store2 = new LocalCrdtStore();
        store1.connect(store2);

        // when:
        final SimpleCrdt crdt1 = store1.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_1);

        // then:
        assertThat(store2.findCrdt(NODE_ID_1).get(), is(crdt1));

        // when:
        final SimpleCrdt crdt2 = store2.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_2);

        // then:
        assertThat(store1.findCrdt(NODE_ID_2).get(), is(crdt2));
    }

    @Test
    public void shouldAddCrdtAfterConnect() {
        // given:
        final LocalCrdtStore store1 = new LocalCrdtStore();
        final LocalCrdtStore store2 = new LocalCrdtStore();
        final SimpleCrdt crdt1 = store1.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_1);
        final SimpleCrdt crdt2 = store2.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_2);

        // then:
        assertThat(store2.findCrdt(NODE_ID_1).isDefined(), is(false));
        assertThat(store1.findCrdt(NODE_ID_2).isDefined(), is(false));

        // when:
        store1.connect(store2);

        // then:
        assertThat(store2.findCrdt(NODE_ID_1).get(), is(crdt1));
        assertThat(store1.findCrdt(NODE_ID_2).get(), is(crdt2));
    }

    @Test
    public void shouldNotAddExistingCrdtAfterConnect() {
        // given:
        final LocalCrdtStore store1 = new LocalCrdtStore();
        final LocalCrdtStore store2 = new LocalCrdtStore();
        store1.connect(store2);
        final SimpleCrdt crdt1 = store1.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_1);
        final SimpleCrdt crdt2 = (SimpleCrdt) store2.findCrdt(NODE_ID_1).get();

        // when:
        store1.disconnect(store2);
        store1.connect(store2);

        // then:
        assertThat(store1.findCrdt(NODE_ID_1).get() == crdt1, is(true));
        assertThat(store2.findCrdt(NODE_ID_1).get() == crdt2, is(true));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendCommandsToConnectedStore() {
        // given:
        final LocalCrdtStore store1 = new LocalCrdtStore();
        final TestSubscriber<CrdtCommand> store1Subscriber = TestSubscriber.create();
        store1.subscribe(store1Subscriber);

        final LocalCrdtStore store2 = new LocalCrdtStore();
        final TestSubscriber<CrdtCommand> store2Subscriber = TestSubscriber.create();
        store2.subscribe(store2Subscriber);

        store1.connect(store2);

        final SimpleCrdt crdt1 = store1.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_1);
        final SimpleCrdt crdt2 = store2.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_2);

        // when:
        final CrdtCommand command1_1 = new CrdtCommand(crdt1.getCrdtId()) {};
        crdt1.sendCommands(command1_1);

        final CrdtCommand command2_1 = new CrdtCommand(crdt2.getCrdtId()) {};
        crdt2.sendCommands(command2_1);

        final CrdtCommand command3_1 = new CrdtCommand(crdt1.getCrdtId()) {};
        final CrdtCommand command3_2 = new CrdtCommand(crdt1.getCrdtId()) {};
        final CrdtCommand command3_3 = new CrdtCommand(crdt1.getCrdtId()) {};
        crdt1.sendCommands(command3_1, command3_2, command3_3);

        final CrdtCommand command4_1 = new CrdtCommand(crdt2.getCrdtId()) {};
        final CrdtCommand command4_2 = new CrdtCommand(crdt2.getCrdtId()) {};
        final CrdtCommand command4_3 = new CrdtCommand(crdt2.getCrdtId()) {};
        crdt2.sendCommands(command4_1, command4_2, command4_3);

        // then:
        store1Subscriber.assertNotComplete();
        store1Subscriber.assertNoErrors();
        store1Subscriber.assertValues(
                new CrdtStore.AddCrdtCommand(crdt1),
                new CrdtStore.AddCrdtCommand(crdt2),
                command1_1,
                command2_1,
                command3_1,
                command3_2,
                command3_3,
                command4_1,
                command4_2,
                command4_3
        );

        store1Subscriber.assertNotComplete();
        store1Subscriber.assertNoErrors();
        store1Subscriber.assertValues(
                new CrdtStore.AddCrdtCommand(crdt1),
                new CrdtStore.AddCrdtCommand(crdt2),
                command1_1,
                command2_1,
                command3_1,
                command3_2,
                command3_3,
                command4_1,
                command4_2,
                command4_3
        );

    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendAllCommandsAfterConnect() {
        // given:
        final LocalCrdtStore store1 = new LocalCrdtStore();
        final TestSubscriber<CrdtCommand> store1Subscriber = TestSubscriber.create();
        store1.subscribe(store1Subscriber);

        final LocalCrdtStore store2 = new LocalCrdtStore();
        final TestSubscriber<CrdtCommand> store2Subscriber = TestSubscriber.create();
        store2.subscribe(store2Subscriber);

        final SimpleCrdt crdt1 = store1.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_1);
        final SimpleCrdt crdt2 = store2.createCrdt(LocalCrdtStoreTest::createCrdt, NODE_ID_2);

        final CrdtCommand command1_1 = new CrdtCommand(crdt1.getCrdtId()) {};
        crdt1.sendCommands(command1_1);

        final CrdtCommand command2_1 = new CrdtCommand(crdt2.getCrdtId()) {};
        crdt2.sendCommands(command2_1);

        final CrdtCommand command3_1 = new CrdtCommand(crdt1.getCrdtId()) {};
        final CrdtCommand command3_2 = new CrdtCommand(crdt1.getCrdtId()) {};
        final CrdtCommand command3_3 = new CrdtCommand(crdt1.getCrdtId()) {};
        crdt1.sendCommands(command3_1, command3_2, command3_3);

        final CrdtCommand command4_1 = new CrdtCommand(crdt2.getCrdtId()) {};
        final CrdtCommand command4_2 = new CrdtCommand(crdt2.getCrdtId()) {};
        final CrdtCommand command4_3 = new CrdtCommand(crdt2.getCrdtId()) {};
        crdt2.sendCommands(command4_1, command4_2, command4_3);

        // when:
        store1.connect(store2);

        // then:
        store1Subscriber.assertNotComplete();
        store1Subscriber.assertNoErrors();
        store1Subscriber.assertValues(
                new CrdtStore.AddCrdtCommand(crdt1),
                command1_1,
                command3_1,
                command3_2,
                command3_3,
                new CrdtStore.AddCrdtCommand(crdt2),
                command2_1,
                command4_1,
                command4_2,
                command4_3
        );

        store2Subscriber.assertNotComplete();
        store2Subscriber.assertNoErrors();
        store2Subscriber.assertValues(
                new CrdtStore.AddCrdtCommand(crdt2),
                command2_1,
                command4_1,
                command4_2,
                command4_3,
                new CrdtStore.AddCrdtCommand(crdt1),
                command1_1,
                command3_1,
                command3_2,
                command3_3
        );
    }

    @Test
    public void shouldSendPartialCommandsAfterReconnect() {

    }
}
