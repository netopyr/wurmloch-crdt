package com.netopyr.wurmloch.integration.examples;

import com.netopyr.wurmloch.crdt.GCounter;
import com.netopyr.wurmloch.store.CrdtStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class GCounterExample {

    @Test
    public void runGCounterExample() {

        // create two CrdtStores and connect them
        final CrdtStore crdtStore1 = new CrdtStore();
        final CrdtStore crdtStore2 = new CrdtStore();
        crdtStore1.connect(crdtStore2);

        // create a G-Counter and find the according replica in the second store
        final GCounter replica1 = crdtStore1.createGCounter("ID_1");
        final GCounter replica2 = crdtStore2.findGCounter("ID_1").get();

        // increment both replicas of the counter
        replica1.increment();
        replica2.increment(2L);

        // the stores are connected, thus the replicas are automatically synchronized
        assertThat(replica1.get(), is(3L));
        assertThat(replica2.get(), is(3L));

        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // increment both counters again
        replica1.increment(3L);
        replica2.increment(5L);

        // the stores are not connected, thus the changes have only local effects
        assertThat(replica1.get(), is(6L));
        assertThat(replica2.get(), is(8L));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // the counter is synchronized automatically and contains now the sum of all increments
        assertThat(replica1.get(), is(11L));
        assertThat(replica2.get(), is(11L));

    }
}
