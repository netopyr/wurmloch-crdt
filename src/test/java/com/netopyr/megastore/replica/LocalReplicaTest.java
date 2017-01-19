package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import io.reactivex.subscribers.TestSubscriber;
import javaslang.control.Option;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LocalReplicaTest {

    public static SimpleCrdt createCrdt(String nodeId, String id, Publisher<? extends CrdtCommand> inCommands, Subscriber<? super CrdtCommand> outCommands) {
        return new SimpleCrdt(id, outCommands);
    }


    @Test
    public void shouldFindCrdts() {
        // given:
        final LocalReplica replica = new LocalReplica();

        // when:
        final Option<? extends Crdt> result1 = replica.findCrdt("ID_1");

        // then:
        assertThat(result1.isDefined(), is(false));

        // when:
        final SimpleCrdt expected = replica.createCrdt(LocalReplicaTest::createCrdt, "ID_1");
        final Option<? extends Crdt> result2 = replica.findCrdt("ID_1");

        // then:
        assertThat(result2.get(), is(expected));
    }

    @Test
    public void shouldAddCrdtWhileConnected() {
        // given:
        final LocalReplica replica1 = new LocalReplica();
        final LocalReplica replica2 = new LocalReplica();
        replica1.connect(replica2);

        // when:
        final SimpleCrdt crdt1 = replica1.createCrdt(LocalReplicaTest::createCrdt, "ID_1");

        // then:
        assertThat(replica2.findCrdt("ID_1").get(), is(crdt1));

        // when:
        final SimpleCrdt crdt2 = replica2.createCrdt(LocalReplicaTest::createCrdt, "ID_2");

        // then:
        assertThat(replica1.findCrdt("ID_2").get(), is(crdt2));
    }

    @Test
    public void shouldAddCrdtAfterConnect() {
        // given:
        final LocalReplica replica1 = new LocalReplica();
        final LocalReplica replica2 = new LocalReplica();
        final SimpleCrdt crdt1 = replica1.createCrdt(LocalReplicaTest::createCrdt, "ID_1");
        final SimpleCrdt crdt2 = replica2.createCrdt(LocalReplicaTest::createCrdt, "ID_2");

        // then:
        assertThat(replica2.findCrdt("ID_1").isDefined(), is(false));
        assertThat(replica1.findCrdt("ID_2").isDefined(), is(false));

        // when:
        replica1.connect(replica2);

        // then:
        assertThat(replica2.findCrdt("ID_1").get(), is(crdt1));
        assertThat(replica1.findCrdt("ID_2").get(), is(crdt2));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendCommandsToConnectedReplica() {
        // given:
        final LocalReplica replica1 = new LocalReplica();
        final TestSubscriber<CrdtCommand> replica1Subscriber = TestSubscriber.create();
        replica1.subscribe(replica1Subscriber);

        final LocalReplica replica2 = new LocalReplica();
        final TestSubscriber<CrdtCommand> replica2Subscriber = TestSubscriber.create();
        replica2.subscribe(replica2Subscriber);

        replica1.connect(replica2);

        final SimpleCrdt crdt1 = replica1.createCrdt(LocalReplicaTest::createCrdt, "ID_1");
        final SimpleCrdt crdt2 = replica2.createCrdt(LocalReplicaTest::createCrdt, "ID_2");

        // when:
        final CrdtCommand command1_1 = new CrdtCommand(crdt1.getId()) {};
        crdt1.sendCommands(command1_1);

        final CrdtCommand command2_1 = new CrdtCommand(crdt2.getId()) {};
        crdt2.sendCommands(command2_1);

        final CrdtCommand command3_1 = new CrdtCommand(crdt1.getId()) {};
        final CrdtCommand command3_2 = new CrdtCommand(crdt1.getId()) {};
        final CrdtCommand command3_3 = new CrdtCommand(crdt1.getId()) {};
        crdt1.sendCommands(command3_1, command3_2, command3_3);

        final CrdtCommand command4_1 = new CrdtCommand(crdt2.getId()) {};
        final CrdtCommand command4_2 = new CrdtCommand(crdt2.getId()) {};
        final CrdtCommand command4_3 = new CrdtCommand(crdt2.getId()) {};
        crdt2.sendCommands(command4_1, command4_2, command4_3);

        // then:
        replica1Subscriber.assertNotComplete();
        replica1Subscriber.assertNoErrors();
        replica1Subscriber.assertValues(
                new AddCrdtCommand(crdt1),
                new AddCrdtCommand(crdt2),
                command1_1,
                command2_1,
                command3_1,
                command3_2,
                command3_3,
                command4_1,
                command4_2,
                command4_3
        );

        replica1Subscriber.assertNotComplete();
        replica1Subscriber.assertNoErrors();
        replica1Subscriber.assertValues(
                new AddCrdtCommand(crdt1),
                new AddCrdtCommand(crdt2),
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
        final LocalReplica replica1 = new LocalReplica();
        final TestSubscriber<CrdtCommand> replica1Subscriber = TestSubscriber.create();
        replica1.subscribe(replica1Subscriber);

        final LocalReplica replica2 = new LocalReplica();
        final TestSubscriber<CrdtCommand> replica2Subscriber = TestSubscriber.create();
        replica2.subscribe(replica2Subscriber);

        final SimpleCrdt crdt1 = replica1.createCrdt(LocalReplicaTest::createCrdt, "ID_1");
        final SimpleCrdt crdt2 = replica2.createCrdt(LocalReplicaTest::createCrdt, "ID_2");

        final CrdtCommand command1_1 = new CrdtCommand(crdt1.getId()) {};
        crdt1.sendCommands(command1_1);

        final CrdtCommand command2_1 = new CrdtCommand(crdt2.getId()) {};
        crdt2.sendCommands(command2_1);

        final CrdtCommand command3_1 = new CrdtCommand(crdt1.getId()) {};
        final CrdtCommand command3_2 = new CrdtCommand(crdt1.getId()) {};
        final CrdtCommand command3_3 = new CrdtCommand(crdt1.getId()) {};
        crdt1.sendCommands(command3_1, command3_2, command3_3);

        final CrdtCommand command4_1 = new CrdtCommand(crdt2.getId()) {};
        final CrdtCommand command4_2 = new CrdtCommand(crdt2.getId()) {};
        final CrdtCommand command4_3 = new CrdtCommand(crdt2.getId()) {};
        crdt2.sendCommands(command4_1, command4_2, command4_3);

        // when:
        replica1.connect(replica2);

        // then:
        replica1Subscriber.assertNotComplete();
        replica1Subscriber.assertNoErrors();
        replica1Subscriber.assertValues(
                new AddCrdtCommand(crdt1),
                command1_1,
                command3_1,
                command3_2,
                command3_3,
                new AddCrdtCommand(crdt2),
                command2_1,
                command4_1,
                command4_2,
                command4_3
        );

        replica2Subscriber.assertNotComplete();
        replica2Subscriber.assertNoErrors();
        replica2Subscriber.assertValues(
                new AddCrdtCommand(crdt2),
                command2_1,
                command4_1,
                command4_2,
                command4_3,
                new AddCrdtCommand(crdt1),
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
