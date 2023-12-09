package database;

import java.util.HashMap;
import java.util.List;        
import java.util.ArrayList;  
import java.util.Comparator; 
import java.time.Instant;
import java.time.format.DateTimeFormatter;


public class ShopList {
    private HashMap<String, Item> items;
    private Instant lastUpdated;

    public ShopList() {
        this.items = new HashMap<String, Item>();
        this.lastUpdated = Instant.now();
    }

    public void setTime(Instant time) {
        this.lastUpdated = time;
    }
    // Add an item to the shop list
    public void addItem(String name, double price, int quantity) {
        Item item = new Item(name, price, quantity);
        items.put(name, item);
    }
    public void addItem(String name, int quantity) {
        Item item = new Item(name, quantity);
        items.put(name, item);
    }

    // Remove an item from the shop list by name
    public void removeItem(String name) {
        items.remove(name);
    }

    // Update the quantity of an item
    public void updateQuantity(String name, int newQuantity) {
        Item item = items.get(name);
        if (newQuantity <= 0) {
            removeItem(name);
        }
        else {
            item.setQuantity(newQuantity);
        }
    }

    public String getTimeStamp() {
        return formatTimeStamp();
    }

    public HashMap<String, Item> getItems() {
        return items;
    }

    // Get the total price of all items in the shop list
    public double getTotalPrice() {
        double total = 0.0;
        for (Item item : items.values()) {
            total += item.getPrice() * item.getQuantity();
        }
        return total;
    }

// Display the items in the shop list in alphabetical order
public void displayItems() {
    if (items.isEmpty()) {
        System.out.println("\nShop list is empty.");
    } else {
        System.out.println("\nShop List:");
        
        // Create a list to hold the items
        List<Item> itemList = new ArrayList<>(items.values());

        // Sort the items alphabetically by name
        itemList.sort(Comparator.comparing(Item::getName));

        // Print the sorted items
        for (Item item : itemList) {
            System.out.println(item.toString());
        }

        System.out.println("Total Price: $" + getTotalPrice());
    }
}

    // Update the last updated timestamp
    private void updateTimeStamp() {
        this.lastUpdated = Instant.now();
    }

    // Timestamp string getter
    public String formatTimeStamp(){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return formatter.format(lastUpdated);
    }

    public static void main(String[] args) {
        // Example usage
        ShopList shopList = new ShopList();

        // Add items to the shop list
        shopList.addItem("Milk", 2.5, 2);
        shopList.addItem("Bread", 1.0, 3);
        shopList.addItem("Eggs", 3.0, 1);

        // Display items and total price
        shopList.displayItems();

        // Update the quantity of an item
        shopList.updateQuantity("Milk", 4);

        // Display updated items and total price
        shopList.displayItems();

        // Remove an item
        shopList.removeItem("Bread");
        shopList.updateTimeStamp();
        // Display updated items and total price
        shopList.displayItems();
    }
}