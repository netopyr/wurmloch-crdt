package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.crdt.GSet;
import com.netopyr.wurmloch.store.LocalCrdtStore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CrdtStoreExample {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        final LocalCrdtStore replica1 = new LocalCrdtStore();
        final LocalCrdtStore replica2 = new LocalCrdtStore();

        final GSet<String> crdtInReplica1 = replica1.createGSet("ID_1");

        assertThat(replica2.findCrdt("ID_1").isEmpty(), is(true));

        replica1.connect(replica2);

        assertThat(replica2.findCrdt("ID_1").isEmpty(), is(false));

        final GSet<String> crdtInReplica2 = (GSet<String>) replica2.findCrdt("ID_1").get();
    }
}
