package crdt;

public class GCounter implements CRDT<Integer> {
    private int value = 0;

    @Override
    public void merge(CRDT<Integer> other) {
        if (other instanceof GCounter) {
            this.value = Math.max(this.value, ((GCounter) other).value);
        } else {
            // Handle incompatible CRDT types
            throw new IllegalArgumentException("Incompatible CRDT types for merge");
        }
    }

    @Override
    public void update(Operation<Integer> operation) {
        if (operation instanceof IncrementOperation) {
            this.value += operation.getValue();
        } else {
            // Handle incompatible operation types
            throw new IllegalArgumentException("Incompatible operation for GCounter");
        }
    }

    @Override
    public Integer query() {
        return this.value;
    }
 
}
