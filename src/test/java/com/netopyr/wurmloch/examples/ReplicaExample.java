package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.crdt.GSet;
import com.netopyr.wurmloch.replica.LocalReplicaStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class ReplicaExample {

    @SuppressWarnings("unchecked")
    @Test
    public static void runReplicaExample() {

        final LocalReplicaStore replica1 = new LocalReplicaStore();
        final LocalReplicaStore replica2 = new LocalReplicaStore();

        final GSet<String> crdtInReplica1 = replica1.createGSet("ID_1");

        assertThat(replica2.findCrdt("ID_1").isEmpty(), is(true));

        replica1.connect(replica2);

        assertThat(replica2.findCrdt("ID_1").isEmpty(), is(false));

        final GSet<String> crdtInReplica2 = (GSet<String>) replica2.findCrdt("ID_1").get();

        assertThat(crdtInReplica1, is(empty()));
        assertThat(crdtInReplica2, is(empty()));
    }
}
