package app;

import database.KeyValueDatabase;
import database.ShopList;

import java.util.Scanner;

public class App {
    private static KeyValueDatabase shoppingLists = new KeyValueDatabase();
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        System.out.println("Welcome to the Shopping List App!");

        while (true) {
            System.out.println("\nMenu:");
            System.out.println("1. Manage a shopping list");
            System.out.println("2. Remove a shopping list");
            System.out.println("3. Exit");

            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            scanner.nextLine(); // Consume the newline character

            switch (choice) {
                case 1:
                    manageShoppingList();
                    break;
                case 2:
                    removeShoppingList();
                    break;
                case 3:
                    System.out.println("\nExiting the Shopping List App. Goodbye!\n");
                    scanner.close();
                    System.exit(0);
                default:
                    System.out.println("\nInvalid choice. Please enter a number between 1 and 3.\n");
            }
        }
    }

    private static void manageShoppingList() {
        System.out.print("\nEnter the ID of the shopping list to manage: ");
        String listId = scanner.nextLine();

        ShopList shopList = (ShopList) shoppingLists.get(listId);

        if (shopList == null) {
            // If the shopping list doesn't exist, create a new one
            shopList = new ShopList();
            shoppingLists.put(listId, shopList);
            System.out.println("\nNew shopping list created with ID: " + listId);
        }

        shopListMenu(shopList);
    }

    private static void removeShoppingList() {
        System.out.print("\nEnter the ID of the shopping list to remove: ");
        String listId = scanner.nextLine();

        if (shoppingLists.containsKey(listId)) {
            shoppingLists.remove(listId);
            System.out.println("\nShopping list with ID '" + listId + "' removed successfully.");
        } else {
            System.out.println("Shopping list with ID '" + listId + "' not found.");
        }
    }

    private static void shopListMenu(ShopList shopList) {
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
                    addItemToShopList(shopList);
                    break;
                case 2:
                    removeItemFromShopList(shopList);
                    break;
                case 3:
                    shopList.displayItems();
                    break;
                case 4:
                    return; // Exit the shop list menu
                default:
                    System.out.println("\nInvalid choice. Please enter a number between 1 and 4.");
            }
        }
    }

    private static void addItemToShopList(ShopList shopList) {
        System.out.print("\nEnter the name of the item: ");
        String itemName = scanner.nextLine();

        System.out.print("\nEnter the quantity of the item: ");
        int quantity = scanner.nextInt();

        shopList.addItem(itemName, quantity);
        System.out.println("Item '" + itemName + "' added to the shopping list.");
    }

    private static void removeItemFromShopList(ShopList shopList) {
        System.out.print("\nEnter the name of the item to remove: ");
        String itemName = scanner.nextLine();

        shopList.removeItem(itemName);
        System.out.println("\nItem '" + itemName + "' removed from the shopping list.");
    }
}
