package com.netopyr.wurmloch.store;

import com.netopyr.wurmloch.crdt.Crdt;
import com.netopyr.wurmloch.crdt.CrdtCommand;
import com.netopyr.wurmloch.store.SimpleCrdt.SimpleCommand;
import io.reactivex.subscribers.TestSubscriber;
import javaslang.control.Option;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CrdtStoreTest {
    
    private static final String NODE_ID_1 = "N_1";
    private static final String NODE_ID_2 = "N_2";
    private static final String CRDT_ID = "ID_1";
    

    @Test
    public void shouldFindCrdts() {
        // given:
        final CrdtStore store = new CrdtStore();
        store.registerFactory(SimpleCrdt.class, SimpleCrdt::new);

        // when:
        final Option<? extends Crdt> result1 = store.findCrdt(NODE_ID_1);

        // then:
        assertThat(result1.isDefined(), is(false));

        // when:
        final SimpleCrdt expected = store.createCrdt(SimpleCrdt.class, NODE_ID_1);
        final Option<? extends Crdt> result2 = store.findCrdt(NODE_ID_1);

        // then:
        assertThat(result2.get(), is(expected));
    }

    @Test
    public void shouldAddCrdtWhileConnected() {
        // given:
        final CrdtStore store1 = new CrdtStore();
        store1.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final CrdtStore store2 = new CrdtStore();
        store2.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        store1.connect(store2);

        // when:
        final SimpleCrdt crdt1 = store1.createCrdt(SimpleCrdt.class, NODE_ID_1);

        // then:
        assertThat(store2.findCrdt(NODE_ID_1).get(), is(crdt1));

        // when:
        final SimpleCrdt crdt2 = store2.createCrdt(SimpleCrdt.class, NODE_ID_2);

        // then:
        assertThat(store1.findCrdt(NODE_ID_2).get(), is(crdt2));
    }

    @Test
    public void shouldAddCrdtAfterConnect() {
        // given:
        final CrdtStore store1 = new CrdtStore();
        store1.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final CrdtStore store2 = new CrdtStore();
        store2.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final SimpleCrdt crdt1 = store1.createCrdt(SimpleCrdt.class, NODE_ID_1);
        final SimpleCrdt crdt2 = store2.createCrdt(SimpleCrdt.class, NODE_ID_2);

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
        final CrdtStore store1 = new CrdtStore();
        store1.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final CrdtStore store2 = new CrdtStore();
        store2.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        store1.connect(store2);
        final SimpleCrdt crdt1 = store1.createCrdt(SimpleCrdt.class, NODE_ID_1);
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
        final CrdtStore store1 = new CrdtStore(NODE_ID_1);
        store1.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final TestSubscriber<CrdtDefinition> store1Subscriber = TestSubscriber.create();
        store1.subscribe(store1Subscriber);

        final CrdtStore store2 = new CrdtStore(NODE_ID_2);
        store2.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final TestSubscriber<CrdtDefinition> store2Subscriber = TestSubscriber.create();
        store2.subscribe(store2Subscriber);

        store1.connect(store2);

        final SimpleCrdt crdt1 = store1.createCrdt(SimpleCrdt.class, CRDT_ID);
        final TestSubscriber<CrdtCommand> crdt1Subscriber = TestSubscriber.create();
        crdt1.subscribe(crdt1Subscriber);

        final SimpleCrdt crdt2 = (SimpleCrdt) store2.findCrdt(CRDT_ID).get();
        final TestSubscriber<CrdtCommand> crdt2Subscriber = TestSubscriber.create();
        crdt2.subscribe(crdt2Subscriber);

        // when:
        final SimpleCommand command1_1 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        crdt1.sendCommands(command1_1);

        final SimpleCommand command2_1 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        crdt2.sendCommands(command2_1);

        final SimpleCommand command3_1 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        final SimpleCommand command3_2 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        final SimpleCommand command3_3 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        crdt1.sendCommands(command3_1, command3_2, command3_3);

        final SimpleCommand command4_1 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        final SimpleCommand command4_2 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        final SimpleCommand command4_3 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        crdt2.sendCommands(command4_1, command4_2, command4_3);

        // then:
        assertThat(store1Subscriber.valueCount(), is(1));
        store1Subscriber.assertNotComplete();
        store1Subscriber.assertNoErrors();

        crdt1Subscriber.assertValues(
                command1_1,
                command2_1,
                command3_1,
                command3_2,
                command3_3,
                command4_1,
                command4_2,
                command4_3
        );
        crdt1Subscriber.assertNotComplete();
        crdt1Subscriber.assertNoErrors();

        assertThat(store2Subscriber.valueCount(), is(1));
        store2Subscriber.assertNotComplete();
        store2Subscriber.assertNoErrors();

        crdt2Subscriber.assertValues(
                command1_1,
                command2_1,
                command3_1,
                command3_2,
                command3_3,
                command4_1,
                command4_2,
                command4_3
        );
        crdt2Subscriber.assertNotComplete();
        crdt2Subscriber.assertNoErrors();

    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendAllCommandsAfterConnect() {
        // given:
        final CrdtStore store1 = new CrdtStore(NODE_ID_1);
        store1.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final TestSubscriber<CrdtDefinition> store1Subscriber = TestSubscriber.create();
        store1.subscribe(store1Subscriber);

        final CrdtStore store2 = new CrdtStore(NODE_ID_2);
        store2.registerFactory(SimpleCrdt.class, SimpleCrdt::new);
        final TestSubscriber<CrdtDefinition> store2Subscriber = TestSubscriber.create();
        store2.subscribe(store2Subscriber);


        final SimpleCrdt crdt1 = store1.createCrdt(SimpleCrdt.class, CRDT_ID);
        final TestSubscriber<CrdtCommand> crdt1Subscriber = TestSubscriber.create();
        crdt1.subscribe(crdt1Subscriber);
        final SimpleCrdt crdt2 = store2.createCrdt(SimpleCrdt.class, CRDT_ID);
        final TestSubscriber<CrdtCommand> crdt2Subscriber = TestSubscriber.create();
        crdt2.subscribe(crdt2Subscriber);

        final SimpleCommand command1_1 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        crdt1.sendCommands(command1_1);

        final SimpleCommand command2_1 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        crdt2.sendCommands(command2_1);

        final SimpleCommand command3_1 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        final SimpleCommand command3_2 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        final SimpleCommand command3_3 = new SimpleCommand(NODE_ID_1, CRDT_ID);
        crdt1.sendCommands(command3_1, command3_2, command3_3);

        final SimpleCommand command4_1 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        final SimpleCommand command4_2 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        final SimpleCommand command4_3 = new SimpleCommand(NODE_ID_2, CRDT_ID);
        crdt2.sendCommands(command4_1, command4_2, command4_3);

        // when:
        store1.connect(store2);

        // then:
        assertThat(store1Subscriber.valueCount(), is(1));
        store1Subscriber.assertNotComplete();
        store1Subscriber.assertNoErrors();

        crdt1Subscriber.assertValues(
                command1_1,
                command3_1,
                command3_2,
                command3_3,
                command2_1,
                command4_1,
                command4_2,
                command4_3
        );
        crdt1Subscriber.assertNotComplete();
        crdt1Subscriber.assertNoErrors();

        assertThat(store2Subscriber.valueCount(), is(1));
        store2Subscriber.assertNotComplete();
        store2Subscriber.assertNoErrors();

        crdt2Subscriber.assertValues(
                command2_1,
                command4_1,
                command4_2,
                command4_3,
                command1_1,
                command3_1,
                command3_2,
                command3_3
        );
        crdt2Subscriber.assertNotComplete();
        crdt2Subscriber.assertNoErrors();
    }

    @Test
    public void shouldSendPartialCommandsAfterReconnect() {

    }
}
