package app;

import java.util.HashMap;
import java.time.Instant;
import java.time.format.DateTimeFormatter;


public class ShopList {
    private HashMap<String, Item> items;
    private Instant lastUpdated;

    public ShopList() {
        this.items = new HashMap<String, Item>();
        this.lastUpdated = Instant.now();
    }

    // Add an item to the shop list
    public void addItem(String name, double price, int quantity) {
        Item item = new Item(name, price, quantity);
        items.put(name, item);
        updateTimeStamp();
    }

    // Remove an item from the shop list by name
    public void removeItem(String name) {
        items.remove(name);
        updateTimeStamp();
    }

    // Update the quantity of an item
    public void updateQuantity(String name, int newQuantity) {
        Item item = items.get(name);
        item.setQuantity(newQuantity);
        updateTimeStamp();
    }

    // Get the total price of all items in the shop list
    public double getTotalPrice() {
        double total = 0.0;
        for (Item item : items.values()) {
            total += item.getPrice() * item.getQuantity();
        }
        return total;
    }

    // Display the items in the shop list
    public void displayItems() {
        if (items.isEmpty()) {
            System.out.println("Shop list is empty.");
        } else {
            System.out.println("Shop List:");
            for (Item item : items.values()) {
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

        // Display updated items and total price
        shopList.displayItems();
    }
}