package crdt;

public class IncrementOperation implements Operation<Integer> {
    private final int value;

    // Constructor
    public IncrementOperation(int value) {
        this.value = value;
    }

    // Getter
    @Override
    public Integer getValue() {
        return value;
    }
}
