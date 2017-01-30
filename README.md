# wurmloch-crdt
Experimental implementations of conflict-free replicated data types (CRDTs) for the JVM

## What is a CRDT?

A conflict-free replicated data type (abbreviated CRDT) can be used in distributed computing to share data between nodes.
It is a data structure with specifc features.
Each node contains its private copy of a CRDT (called replica), which it modifies locally without the need to synchronize immediately.
However, when two replicas are connected, they can synchronize automatically in the background.
To achieve that, CRDTs are designed in such a way that there can never be any conflicts between concurrent updates.

## How can I use wurmloch-crdt?

_TBD_

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

### G-Counter

### PN-Counter

### LWW-Register

### MV-Register

### OR-Set

### RGA

## Further Readings

* Wikipedia: [Conflict-free replicated data type][wikipedia crdt]
* [A comprehensive study of Convergent and Commutative Replicated Data Types][crdt article]

[wikipedia crdt]: https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type
[crdt article]: http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf
