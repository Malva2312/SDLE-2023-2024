package app;

import database.ShopList;

import java.util.Scanner;
import java.time.Instant;

public class App {
    private Backend backend; 
    private Scanner scanner;
    App(){
        backend = new Backend();
        scanner = new Scanner(System.in);
    }
    void run(){
        System.out.println("Welcome to the Shopping List App!");

        while (true) {
            System.out.println("\nMenu:");
            System.out.println("1. Manage a shopping list");
            System.out.println("2. Exit");

            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine(); // Consume the newline character

            switch (choice) {
                case 1:
                    manageShoppingList();
                    break;
                case 2:
                    System.out.println("\nExiting the Shopping List App. Goodbye!\n");
                    scanner.close();
                    System.exit(0);
                default:
                    System.out.println("\nInvalid choice. Please enter a number between 1 and 3.\n");
            }
        }
    }
    public static void main(String[] args) {
        App app = new App();
        app.run();
    }

    private void manageShoppingList() {
        System.out.print("\nEnter the ID of the shopping list to manage: ");
        String listId = scanner.nextLine();

        ShopList shopList =  backend.getShopList(listId);

        if (shopList == null) {
            // If the shopping list doesn't exist, create a new one
            shopList = new ShopList();
            backend.setShopList(listId, shopList);
            System.out.println("\nNew shopping list created with ID: " + listId);
        }

        shopListMenu(listId);
    }

    private void shopListMenu(String key) {
        while (true) {
            System.out.println("\nShopping List Menu:");
            System.out.println("1. Add an item");
            System.out.println("2. Remove an item");
            System.out.println("3. Display items");
            System.out.println("4. Back to main menu");

            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine(); // Consume the newline character

            switch (choice) {
                case 1:
                    addItemToShopList(key);
                    break;
                case 2:
                    removeItemFromShopList(key);
                    break;
                case 3:
                    backend.getShopList(key).displayItems();
                    break;
                case 4:
                    return; // Exit the shop list menu
                default:
                    System.out.println("\nInvalid choice. Please enter a number between 1 and 4.");
            }
        }
    }

    private void addItemToShopList(String key) {
        System.out.print("\nEnter the name of the item: ");
        String itemName = scanner.nextLine();

        System.out.print("\nEnter the quantity of the item: ");
        int quantity = scanner.nextInt();

        ShopList shopList = backend.getShopList(key);
        shopList.addItem(itemName, quantity);
        shopList.setTimeStamp(Instant.now());
        shopList = backend.setShopList(key, shopList);
        System.out.println("Item '" + itemName + "' added to the shopping list.");
    }

    private void removeItemFromShopList(String key) {
        System.out.print("\nEnter the name of the item to remove: ");
        String itemName = scanner.nextLine();

        ShopList shopList = backend.getShopList(key);
        if (!shopList.getItems().containsKey(itemName)) {
            System.out.println("\nItem '" + itemName + "' not found in the shopping list.");
            return;
        }
        else{
        System.out.print("\nEnter the quantity of the item to remove: ");
        int quantity = scanner.nextInt();

        int newQuantity = shopList.getItems().get(itemName).getQuantity() - quantity;

        shopList.updateQuantity(itemName, newQuantity);
        shopList.setTimeStamp(Instant.now());
        shopList = backend.setShopList(key, shopList);
        System.out.println("\nItem '" + itemName + "' x" + quantity +" removed from the shopping list.");
        }
    }
}
