package com.netopyr.wurmloch.integration.examples;

import com.netopyr.wurmloch.store.CrdtStore;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CrdtStoreExample {

    @Test
    public void runCrdtStoreExample() {
        // create two CrdtStores
        final CrdtStore crdtStore1 = new CrdtStore();
        final CrdtStore crdtStore2 = new CrdtStore();

        // create a new G-Set
        crdtStore1.createGSet("ID_1");

        // at this point the CrdtStores are not connected, therefore the new G-Set is unknown in the second store
        assertThat(crdtStore2.findCrdt("ID_1").isDefined(), is(false));

        // connect both stores
        crdtStore1.connect(crdtStore2);

        // now the new G-Set is also known in the second store
        assertThat(crdtStore2.findCrdt("ID_1").isDefined(), is(true));
    }
}
