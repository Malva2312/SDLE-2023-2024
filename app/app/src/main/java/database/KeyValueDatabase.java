package database;

import java.util.HashMap;

public class KeyValueDatabase {
    private HashMap<Long, Object> database;

    public KeyValueDatabase() {
        this.database = new HashMap<>();
    }

    // Put a key-value pair into the database
    public void put(Long key, Object value) {
        database.put(key, value);
    }

    // Get the value associated with a key
    public Object get(Long key) {
        return database.get(key);
    }

    // Remove a key-value pair from the database
    public void remove(Long key) {
        database.remove(key);
    }

    // Check if the database contains a key
    public boolean containsKey(Long key) {
        return database.containsKey(key);
    }

    // Get the size of the database
    public int size() {
        return database.size();
    }

    // Clear all key-value pairs from the database
    public void clear() {
        database.clear();
    }

    public static void main(String[] args) {
        // Example usage
        KeyValueDatabase kvdb = new KeyValueDatabase();

        // Keys
        Long key1 = (long) 1;
        Long key2 = (long) 2;
        Long key3 = (long) 3;
        Long key4 = (long) 4;


        // Put key-value pairs
        kvdb.put( key1, "John");
        kvdb.put( key2, 25);
        kvdb.put( key3, "ExampleCity");

        // Get values by keys
        System.out.println("Name: " + kvdb.get(key1));
        System.out.println("Age: " + kvdb.get(key2));
        System.out.println("City: " + kvdb.get(key3));

        // Check if a key exists
        System.out.println("Contains key 'name': " + kvdb.containsKey(key1));
        System.out.println("Contains key 'gender': " + kvdb.containsKey(key4));

        // Remove a key-value pair
        kvdb.remove(key2);
        System.out.println("After removing 'age', size: " + kvdb.size());

        // Clear the database
        kvdb.clear();
        System.out.println("After clearing, size: " + kvdb.size());
    }
}
