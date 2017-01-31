package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.crdt.PNCounter;
import com.netopyr.wurmloch.store.LocalCrdtStore;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.is;

public class PNCounterExample {

    public static void main(String[] args) {

        // create two LocalCrdtStores and connect them
        final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
        final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
        crdtStore1.connect(crdtStore2);

        // create a PN-Counter and find the according replica in the second store
        final PNCounter replica1 = crdtStore1.createPNCounter("ID_1");
        final PNCounter replica2 = crdtStore2.findPNCounter("ID_1").get();

        // change the value of both replicas of the counter
        replica1.increment();
        replica2.decrement(2L);

        // the stores are connected, thus the replicas are automatically synchronized
        MatcherAssert.assertThat(replica1.get(), is(-1L));
        MatcherAssert.assertThat(replica2.get(), is(-1L));

        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // update both counters again
        replica1.decrement(3L);
        replica2.increment(5L);

        // the stores are not connected, thus the changes have only local effects
        MatcherAssert.assertThat(replica1.get(), is(-4L));
        MatcherAssert.assertThat(replica2.get(), is(4L));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // the counter is synchronized automatically and contains now the sum of all increments minus all decrements
        MatcherAssert.assertThat(replica1.get(), is(1L));
        MatcherAssert.assertThat(replica2.get(), is(1L));

    }
}
