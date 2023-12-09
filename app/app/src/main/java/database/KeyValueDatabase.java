package database;

import java.util.HashMap;

public class KeyValueDatabase {
    private HashMap<String, Object> database;

    public KeyValueDatabase() {
        this.database = new HashMap<>();
    }

    // Put a key-value pair into the database
    public void put(String key, Object value) {
        database.put(key, value);
    }

    // Get the value associated with a key
    public Object get(String key) {
        return database.get(key);
    }

    // Remove a key-value pair from the database
    public void remove(String key) {
        database.remove(key);
    }

    // Check if the database contains a key
    public boolean containsKey(String key) {
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

        // Put key-value pairs
        kvdb.put( "name", "John");
        kvdb.put( "age", 25);
        kvdb.put( "city", "ExampleCity");

        // Get values by keys
        System.out.println("Name: " + kvdb.get("name"));
        System.out.println("Age: " + kvdb.get("age"));
        System.out.println("City: " + kvdb.get("city"));

        // Check if a key exists
        System.out.println("Contains key 'name': " + kvdb.containsKey("name"));
        System.out.println("Contains key 'gender': " + kvdb.containsKey("gender"));

        // Remove a key-value pair
        kvdb.remove("age");
        System.out.println("After removing 'age', size: " + kvdb.size());

        // Clear the database
        kvdb.clear();
        System.out.println("After clearing, size: " + kvdb.size());
    }
}
