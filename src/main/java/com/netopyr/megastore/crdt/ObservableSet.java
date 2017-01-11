package com.netopyr.megastore.crdt;

import io.reactivex.Observable;

import java.util.Set;

public interface ObservableSet<T> extends Set<T> {

    Observable<Change> onChange();

    interface Change<T> {
        Set<T> getRemoved();
        Set<T> getAdded();
    }
}
