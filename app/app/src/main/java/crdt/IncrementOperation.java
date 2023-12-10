package crdt;

public class IncrementOperation implements Operation<Integer> {
    private int value;

    // Constructor, getters, setters, etc.

    @Override
    public Integer getValue() {
        return value;
    }
}
