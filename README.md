# wurmloch-crdt
Experimental implementations of conflict-free replicated data types (CRDTs) for the JVM

[![Bintray](https://img.shields.io/bintray/v/netopyr/wurmloch/wurmloch-crdt.svg?colorB=0081c4)](https://bintray.com/netopyr/wurmloch/wurmloch-crdt)
[![Maven](https://img.shields.io/maven-central/v/com.netopyr.wurmloch/wurmloch-crdt.svg)](https://search.maven.org/#search|ga|1|com.netopyr.wurmloch)


## What is a CRDT?

A conflict-free replicated data type (abbreviated CRDT) can be used in distributed computing to share data between nodes.
It is a data structure with specifc features.
Each node contains its private copy of a CRDT (called replica), which it modifies locally without the need to synchronize immediately.
However, when two replicas are connected, they can synchronize automatically in the background.
To achieve that, CRDTs are designed in such a way that there can never be any conflicts between concurrent updates.

## Adding wurmloch-crdt to your project

### Maven
```xml
  <dependency>
    <groupId>com.netopyr.wurmloch</groupId>
    <artifactId>wurmloch-crdt</artifactId>
    <version>0.1.0</version>
  </dependency>
```

### Gradle
```groovy
  dependencies {
      compile 'com.netopyr.wurmloch:wurmloch-crdt:0.1.0'
  }
```

## Examples

The following section explains the CRDTs available in wurmloch-crdt and shows examples of their usage.

### CrdtStore-Example

In wurmloch-crdt each node of a distributed application contains a CrdtStore that manages the CRDTs.
It offers functionality to add CRDTs and find CRDTs that were added by other nodes.

To be able to identify CRDTs, they have to be created with a unique ID.
If no ID is provided, a random UUID is used.

In real-world scenarios, the CrdtStores would run in different nodes on different JVMs and if they are connected depends solely on the status of the network.
But wurmloch-crdt also contains a local implementation LocalCrdtStore, which connection state can be controlled by calling methods connect() and disconnect().

The following examples creates two LocalCrdtStores in which our first CRDT, a G-Set, is created.

```java
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
```
_Code Sample 1: Using the LocalCrdtStore (see [CrdtStoreExample][class crdtstoreexample])_

### G-Set 

A G-Set or Grow-Only Set is a Set to which elements can only be added and never removed.
It is probably the simplest CRDT.
Synchronizing two G-Set that have diverged is accomplished by calculating the union of both Sets.

On first sight, a Set where elements can never be removed might seem superfluous.
But there are actually a lot of use cases, where such a limited Set is useful.
For example most domain entities in business applications are never really removed for auditing reasons and therefore could be stored in a G-Set.
Also it is a common practice to build complex CRDTs upon simple CRDTs.
All of the more complex CRDT-Sets, that also allow removes, are built on top of G-Sets.

```java
    // create two LocalCrdtStores and connect them
    final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
    crdtStore1.connect(crdtStore2);
    
    // create a G-Set and find the according replica in the second store
    final GSet<String> replica1 = crdtStore1.createGSet("ID_1");
    final GSet<String> replica2 = crdtStore2.<String>findGSet("ID_1").get();
    
    // add one entry to each replica
    replica1.add("apple");
    replica2.add("banana");
    
    // the stores are connected, thus the G-Set is automatically synchronized
    MatcherAssert.assertThat(replica1, Matchers.containsInAnyOrder("apple", "banana"));
    MatcherAssert.assertThat(replica2, Matchers.containsInAnyOrder("apple", "banana"));
    
    // disconnect the stores simulating a network issue, offline mode etc.
    crdtStore1.disconnect(crdtStore2);
    
    // add one entry to each replica
    replica1.add("strawberry");
    replica2.add("pear");
    
    // the stores are not connected, thus the changes have only local effects
    MatcherAssert.assertThat(replica1, Matchers.containsInAnyOrder("apple", "banana", "strawberry"));
    MatcherAssert.assertThat(replica2, Matchers.containsInAnyOrder("apple", "banana", "pear"));
    
    // reconnect the stores
    crdtStore1.connect(crdtStore2);
    
    // the G-Set is synchronized automatically and contains now all elements
    MatcherAssert.assertThat(replica1, Matchers.containsInAnyOrder("apple", "banana", "strawberry", "pear"));
    MatcherAssert.assertThat(replica2, Matchers.containsInAnyOrder("apple", "banana", "strawberry", "pear"));
```
_Code Sample 2: Using a G-Set (see [GSetExample][class gsetexample])_

### G-Counter

A G-Counter or increment-only Counter is - as the name suggests - an integer counter, that one can only incremented.
It has methods to increment and request the current value.
When synchronized, the value converges towards the sum of all increments.

```java
    // create two LocalCrdtStores and connect them
    final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
    crdtStore1.connect(crdtStore2);
    
    // create a G-Counter and find the according replica in the second store
    final GCounter replica1 = crdtStore1.createGCounter("ID_1");
    final GCounter replica2 = crdtStore2.findGCounter("ID_1").get();
    
    // increment both replicas of the counter
    replica1.increment();
    replica2.increment(2L);
    
    // the stores are connected, thus the replicas are automatically synchronized
    MatcherAssert.assertThat(replica1.get(), is(3L));
    MatcherAssert.assertThat(replica2.get(), is(3L));
    
    // disconnect the stores simulating a network issue, offline mode etc.
    crdtStore1.disconnect(crdtStore2);
    
    // increment both counters again
    replica1.increment(3L);
    replica2.increment(5L);
    
    // the stores are not connected, thus the changes have only local effects
    MatcherAssert.assertThat(replica1.get(), is(6L));
    MatcherAssert.assertThat(replica2.get(), is(8L));
    
    // reconnect the stores
    crdtStore1.connect(crdtStore2);
    
    // the counter is synchronized automatically and contains now the sum of all increments
    MatcherAssert.assertThat(replica1.get(), is(11L));
    MatcherAssert.assertThat(replica2.get(), is(11L));
```
_Code Sample 3: Using a G-Counter (see [GCounterExample][class gcounterexample])_

### PN-Counter

A PN-Counter is an integer-counter, that can be incremented and decremented.
When synchronized, the value converges towards the sum of all increments minus the sum of all decrements.

```java
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
```
_Code Sample 4: Using a PN-Counter (see [PNCounterExample][class pncounterexample])_

### LWW-Register

A Register stores a single Object.
It contains a get()- and a set()-method to read and write the value.
In an LWW-Register (Last-Writer-Wins Register), the last set-call will supersede previous calls.
Internally a LWWRegister uses a [VectorClock][wiki vectorclock] to keep track of the time.
If two updates happen concurrently in disconnected replicas, the one from the store with the smaller Id will take precedence.

Please note that a last-writer-wins strategy results in data loss, if data is modified concurrently.
This is ok in some use-cases, but has to be avoided in others.
MV-Registers (see below) provide a much more sophisticated logic for these kind of cases.

```java
    // create two LocalCrdtStores and connect them
    final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
    crdtStore1.connect(crdtStore2);
    
    // create an LWW-Register and find the according replica in the second store
    final LWWRegister<String> replica1 = crdtStore1.createLWWRegister("ID_1");
    final LWWRegister<String> replica2 = crdtStore2.<String>findLWWRegister("ID_1").get();
    
    // set values in both replicas
    replica1.set("apple");
    replica2.set("banana");
    
    // the stores are connected, thus the last write wins
    assertThat(replica1.get(), is("banana"));
    assertThat(replica2.get(), is("banana"));
    
    
    // disconnect the stores simulating a network issue, offline mode etc.
    crdtStore1.disconnect(crdtStore2);
    
    // add one entry to each replica
    replica1.set("strawberry");
    replica2.set("pear");
    
    // the stores are not connected, thus the changes have only local effects
    assertThat(replica1.get(), is("strawberry"));
    assertThat(replica2.get(), is("pear"));
    
    // reconnect the stores
    crdtStore1.connect(crdtStore2);
    
    // the LWW-Register is synchronized automatically.
    // as the update happened concurrently, the update from the node with the smaller Id wins
    assertThat(replica1.get(), is("strawberry"));
    assertThat(replica2.get(), is("strawberry"));
```
_Code Sample 5: Using an LWW-Register (see [LWWRegisterExample][class lwwregisterexample])_

### MV-Register

An MV-Register (Multi-Value Register) is another implementation of a register.
It avoids the kind of data loss, that is inherent to any kind of last-writer-wins strategy.
Instead if the value of a MV-Register is changed concurrently, it keeps all values.
Therefore the result of the get()-method is a collection.

```java
    // create two LocalCrdtStores and connect them
    final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
    crdtStore1.connect(crdtStore2);

    // create an MV-Register and find the according replica in the second store
    final MVRegister<String> replica1 = crdtStore1.createMVRegister("ID_1");
    final MVRegister<String> replica2 = crdtStore2.<String>findMVRegister("ID_1").get();

    // set values in both replicas
    replica1.set("apple");
    replica2.set("banana");

    // the stores are connected, thus we can determine the order of both writes
    // the latter write overrides the previous one
    assertThat(replica1.get(), contains("banana"));
    assertThat(replica2.get(), contains("banana"));

    // disconnect the stores simulating a network issue, offline mode etc.
    crdtStore1.disconnect(crdtStore2);

    // change the value in both replicas
    replica1.set("strawberry");
    replica2.set("pear");

    // the stores are not connected, thus the changes have only local effects
    assertThat(replica1.get(), contains("strawberry"));
    assertThat(replica2.get(), contains("pear"));

    // reconnect the stores
    crdtStore1.connect(crdtStore2);

    // as the update happened concurrently, we cannot determine an order and both values are kept
    assertThat(replica1.get(), containsInAnyOrder("strawberry", "pear"));
    assertThat(replica2.get(), containsInAnyOrder("strawberry", "pear"));

    // update the value one more time
    replica2.set("orange");

    // the last update was clearly after the concurrent ones, therefore both replicas contain the last value only
    assertThat(replica1.get(), contains("orange"));
    assertThat(replica2.get(), contains("orange"));
```
_Code Sample 6: Using an MV-Register (see [MVRegisterExample][class mvregisterexample])_

Note that a MV-Register is not a Set.
As can be seen in the following more complex example, an MV-Register keeps track of which values were overriden and can be eliminated.

```java
    // create three LocalCrdtStores and connect them
    final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore3 = new LocalCrdtStore();
    crdtStore2.connect(crdtStore1);
    crdtStore2.connect(crdtStore3);

    // create an MV-Register and find the according replica in the other stores
    final MVRegister<String> replica1 = crdtStore1.createMVRegister("ID_1");
    final MVRegister<String> replica2 = crdtStore2.<String>findMVRegister("ID_1").get();
    final MVRegister<String> replica3 = crdtStore3.<String>findMVRegister("ID_1").get();

    // disconnect store 2 and 3
    crdtStore2.disconnect(crdtStore3);

    // set some values
    replica1.set("apple");
    replica3.set("banana");

    // store 1 and 2 contain "apple", store 3 contains "banana"
    assertThat(replica1.get(), containsInAnyOrder("apple"));
    assertThat(replica2.get(), containsInAnyOrder("apple"));
    assertThat(replica3.get(), containsInAnyOrder("banana"));

    // disconnect store 1 and 2 and connect store 2 and 3 instead
    crdtStore2.disconnect(crdtStore1);
    crdtStore2.connect(crdtStore3);

    // set the register in store 1 to "strawberry"
    replica1.set("strawberry");

    // store 1 still contains "strawberry" only
    // store 2 and 3 are synchronized and contain "apple" and "banana", because these updates happened concurrently
    assertThat(replica1.get(), containsInAnyOrder("strawberry"));
    assertThat(replica2.get(), containsInAnyOrder("apple", "banana"));
    assertThat(replica3.get(), containsInAnyOrder("apple", "banana"));

    // connect all stores again
    crdtStore2.connect(crdtStore1);

    // the result is not simply the union of all values
    // "apple" was overridden by "strawberry", therefore it disappears now
    // "banana" and "strawberry" were set concurrently, thus both values are still in the result
    assertThat(replica1.get(), containsInAnyOrder("banana", "strawberry"));
    assertThat(replica2.get(), containsInAnyOrder("banana", "strawberry"));
    assertThat(replica3.get(), containsInAnyOrder("banana", "strawberry"));
```
_Code Sample 7: A more complex example using an MV-Register (see [MVRegisterComplexExample][class mvregistercomplexexample])_

### OR-Set

The OR-Set (Observed-Remove Set) is a CRDT which probably comes closes to the expected behavior of a Set.
The basic idea is, that only elements which add-operation is visible to a replica can be removed from that replica.
That means for example, if an element is added in one replica and at the same time removed from a second, non-synchronized replica, it is still contained in the OR-Set, because the add-operation was not visible to the second replica yet.

```java
    // create two LocalCrdtStores and connect them
    final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
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
```
_Code Sample 8: Using an OR-Set (see [ORSetExample][class orsetexample])_

### RGA

An RGA (Replicated Growable Array) is a CRDT that behaves similar to a List.
The elements have an order and one can add and remove elements at specific positions.
In this implementation, it is not possible though to set values, because it is not defined how concurrent sets should behave.
An UnsupportedOperationException will be thrown if the set() method is called.

```java
    // create two LocalCrdtStores and connect them
    final LocalCrdtStore crdtStore1 = new LocalCrdtStore();
    final LocalCrdtStore crdtStore2 = new LocalCrdtStore();
    crdtStore1.connect(crdtStore2);

    // create a G-Set and find the according replica in the second store
    final RGA<String> replica1 = crdtStore1.createRGA("ID_1");
    final RGA<String> replica2 = crdtStore2.<String>findRGA("ID_1").get();

    // add one entry to each replica
    replica1.add("apple");
    replica2.add("banana");

    // the stores are connected, thus the RGA is automatically synchronized
    assertThat(replica1, contains("apple", "banana"));
    assertThat(replica2, contains("apple", "banana"));

    // disconnect the stores simulating a network issue, offline mode etc.
    crdtStore1.disconnect(crdtStore2);

    // add one entry to each replica
    replica1.remove("banana");
    replica2.add(1, "strawberry");

    // the stores are not connected, thus the changes have only local effects
    assertThat(replica1, containsInAnyOrder("apple"));
    assertThat(replica2, containsInAnyOrder("apple", "strawberry", "banana"));

    // reconnect the stores
    crdtStore1.connect(crdtStore2);

    // the RGA is synchronized automatically
    assertThat(replica1, containsInAnyOrder("apple", "strawberry"));
    assertThat(replica2, containsInAnyOrder("apple", "strawberry"));

    // disconnect the stores
    crdtStore1.disconnect(crdtStore2);

    // set() is not supported in an RGA
    // if we try to simulate with a remove and add, we can see the problem
    replica1.remove(0);
    replica1.add("pear");
    replica2.remove(0);
    replica2.add("orange");

    // the first entry has been replaced
    assertThat(replica1, containsInAnyOrder("pear", "strawberry"));
    assertThat(replica2, containsInAnyOrder("orange", "strawberry"));

    // reconnect the stores
    crdtStore1.connect(crdtStore2);

    // we have actually added two elements, the RGA keeps both
    assertThat(replica1, containsInAnyOrder("orange", "pear", "strawberry"));
    assertThat(replica2, containsInAnyOrder("orange", "pear", "strawberry"));
```
_Code Sample 9: Using an RGA (see [RGAExample][class rgaexample])_

## Further Readings

* Wikipedia: [Conflict-free replicated data type][wikipedia crdt]
* [A comprehensive study of Convergent and Commutative Replicated Data Types][crdt article]

[wikipedia crdt]: https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type
[crdt article]: http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf
[wiki vectorclock]: https://en.wikipedia.org/wiki/Vector_clocks

[class crdtstoreexample]: src/main/java/com/netopyr/wurmloch/examples/CrdtStoreExample.java
[class gsetexample]: src/main/java/com/netopyr/wurmloch/examples/GSetExample.java
[class gcounterexample]: src/main/java/com/netopyr/wurmloch/examples/GCounterExample.java
[class pncounterexample]: src/main/java/com/netopyr/wurmloch/examples/PNCounterExample.java
[class lwwregisterexample]: src/main/java/com/netopyr/wurmloch/examples/LWWRegisterExample.java
[class mvregisterexample]: src/main/java/com/netopyr/wurmloch/examples/MVRegisterExample.java
[class mvregistercomplexexample]: src/main/java/com/netopyr/wurmloch/examples/MVRegisterComplexExample.java
[class orsetexample]: src/main/java/com/netopyr/wurmloch/examples/ORSetExample.java
[class rgaexample]: src/main/java/com/netopyr/wurmloch/examples/RGAExample.java
