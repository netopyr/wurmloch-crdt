package com.netopyr.wurmloch.examples;

import com.netopyr.wurmloch.store.LocalCrdtStore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CrdtStoreExample {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        // create two LocalCrdtStores
        final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
        final LocalCrdtStore crdtStore2 = new LocalCrdtStore();

        // create a new G-Set
        crdtStore1.createGSet("ID_1");

        // at this point the LocalCrdtStores are not connected, therefore the new G-Set is unknown in the second store
        assertThat(crdtStore2.findCrdt("ID_1").isDefined(), is(false));

        // connect both stores
        crdtStore1.connect(crdtStore2);

        // now the new G-Set is also known in the second store
        assertThat(crdtStore2.findCrdt("ID_1").isDefined(), is(true));
    }
}
