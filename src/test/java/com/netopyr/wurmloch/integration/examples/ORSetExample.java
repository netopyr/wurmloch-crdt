package com.netopyr.wurmloch.integration.examples;

import com.netopyr.wurmloch.crdt.ORSet;
import com.netopyr.wurmloch.store.CrdtStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class ORSetExample {

    @Test
    public void runORSetExample() {

        // create two LocalCrdtStores and connect them
        final CrdtStore crdtStore1 = new CrdtStore();
        final CrdtStore crdtStore2 = new CrdtStore();
        crdtStore1.connect(crdtStore2);

        // create a G-Set and find the according replica in the second store
        final ORSet<String> replica1 = crdtStore1.createORSet("ID_1");
        final ORSet<String> replica2 = crdtStore2.<String>findORSet("ID_1").get();

        // add one entry to each replica
        replica1.add("apple");
        replica2.add("banana");

        // the stores are connected, thus the G-Set is automatically synchronized
        assertThat(replica1, containsInAnyOrder("apple", "banana"));
        assertThat(replica2, containsInAnyOrder("apple", "banana"));

        // disconnect the stores simulating a network issue, offline mode etc.
        crdtStore1.disconnect(crdtStore2);

        // remove one of the entries
        replica1.remove("banana");
        replica2.add("strawberry");

        // the stores are not connected, thus the changes have only local effects
        assertThat(replica1, containsInAnyOrder("apple"));
        assertThat(replica2, containsInAnyOrder("apple", "banana", "strawberry"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // "banana" was added before both stores got disconnected, therefore it is now removed during synchronization
        assertThat(replica1, containsInAnyOrder("apple", "strawberry"));
        assertThat(replica2, containsInAnyOrder("apple", "strawberry"));

        // disconnect the stores again
        crdtStore1.disconnect(crdtStore2);

        // add one entry to each replica
        replica1.add("pear");
        replica2.add("pear");
        replica2.remove("pear");

        // "pear" was added in both stores concurrently, but immediately removed from replica2
        assertThat(replica1, containsInAnyOrder("apple", "strawberry", "pear"));
        assertThat(replica2, containsInAnyOrder("apple", "strawberry"));

        // reconnect the stores
        crdtStore1.connect(crdtStore2);

        // "pear" was added in both replicas concurrently
        // this means that the add-operation of "pear" to replica1 was not visible to replica2
        // therefore removing "pear" from replica2 does not include removing "pear" from replica1
        // as a result "pear" reappears in the merged Sets
        assertThat(replica1, containsInAnyOrder("apple", "strawberry", "pear"));
        assertThat(replica2, containsInAnyOrder("apple", "strawberry", "pear"));

    }
}
