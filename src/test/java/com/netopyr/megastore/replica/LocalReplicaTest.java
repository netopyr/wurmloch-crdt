package com.netopyr.megastore.replica;

import com.netopyr.megastore.crdt.Crdt;
import com.netopyr.megastore.crdt.CrdtCommand;
import io.reactivex.observers.TestObserver;
import javaslang.control.Option;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LocalReplicaTest {

    @Test
    public void shouldFindCrdts() {
        // given:
        final LocalReplica replica = new LocalReplica();

        // when:
        final Option<? extends Crdt> result1 = replica.find("ID_1");

        // then:
        assertThat(result1.isDefined(), is(false));

        // given:
        final Crdt expected = new SimpleCrdt("ID_1");

        // when:
        replica.register(expected);
        final Option<? extends Crdt> result2 = replica.find("ID_1");

        // then:
        assertThat(result2.get(), is(expected));
    }

    @Test
    public void shouldAddCrdtWhileConnected() {
        // given:
        final LocalReplica replica1 = new LocalReplica();
        final LocalReplica replica2 = new LocalReplica();
        replica1.connect(replica2);
        final Crdt crdt1 = new SimpleCrdt("ID_1");
        final Crdt crdt2 = new SimpleCrdt("ID_2");

        // when:
        replica1.register(crdt1);

        // then:
        assertThat(replica2.find("ID_1").get(), is(crdt1));

        // when:
        replica2.register(crdt2);

        // then:
        assertThat(replica1.find("ID_2").get(), is(crdt2));
    }

    @Test
    public void shouldAddCrdtAfterConnect() {
        // given:
        final LocalReplica replica1 = new LocalReplica();
        final LocalReplica replica2 = new LocalReplica();
        final Crdt crdt1 = new SimpleCrdt("ID_1");
        final Crdt crdt2 = new SimpleCrdt("ID_2");
        replica1.register(crdt1);
        replica2.register(crdt2);

        // then:
        assertThat(replica2.find("ID_1").isDefined(), is(false));
        assertThat(replica1.find("ID_2").isDefined(), is(false));

        // when:
        replica1.connect(replica2);

        // then:
        assertThat(replica2.find("ID_1").get(), is(crdt1));
        assertThat(replica1.find("ID_2").get(), is(crdt2));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendCommandsToConnectedReplica() {
        // given:
        final LocalReplica replica1 = new LocalReplica();
        final TestObserver<CrdtCommand> replica1Observer = new TestObserver<>();
        replica1.onCommands().subscribe(replica1Observer);
        final SimpleCrdt crdt1 = new SimpleCrdt("ID1");

        final LocalReplica replica2 = new LocalReplica();
        final TestObserver<CrdtCommand> replica2Observer = new TestObserver<>();
        replica2.onCommands().subscribe(replica2Observer);
        final SimpleCrdt crdt2 = new SimpleCrdt("ID2");

        replica1.connect(replica2);

        replica1.register(crdt1);
        replica2.register(crdt2);

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
        replica1Observer.assertNotComplete();
        replica1Observer.assertNoErrors();
        replica1Observer.assertValues(
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

        replica1Observer.assertNotComplete();
        replica1Observer.assertNoErrors();
        replica1Observer.assertValues(
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
        final TestObserver<CrdtCommand> replica1Observer = new TestObserver<>();
        replica1.onCommands().subscribe(replica1Observer);
        final SimpleCrdt crdt1 = new SimpleCrdt("ID1");

        final LocalReplica replica2 = new LocalReplica();
        final TestObserver<CrdtCommand> replica2Observer = new TestObserver<>();
        replica2.onCommands().subscribe(replica2Observer);
        final SimpleCrdt crdt2 = new SimpleCrdt("ID2");

        replica1.register(crdt1);
        replica2.register(crdt2);

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
        replica1Observer.assertNotComplete();
        replica1Observer.assertNoErrors();
        replica1Observer.assertValues(
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

        replica2Observer.assertNotComplete();
        replica2Observer.assertNoErrors();
        replica2Observer.assertValues(
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
