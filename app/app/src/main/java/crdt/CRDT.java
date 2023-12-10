package crdt;

import crdt.Operation;

public interface CRDT<T> {
    void merge(CRDT<T> other);
    void update(Operation<T> operation);
    T query();
}