package crdt;

import java.util.HashMap;
import java.util.Map;

public class ShopListCRDT extends GCounter {
    private Map<String, Integer> items;

    public ShopListCRDT() {
        super();
        this.items = new HashMap<>();
    }

    public void addItem(String itemName, int quantity) {
      //  super.increment();
        items.put(itemName, items.getOrDefault(itemName, 0) + quantity);
    }

    public void removeItem(String itemName, int quantity) {
        if (items.containsKey(itemName)) {
            int currentQuantity = items.get(itemName);
            int newQuantity = Math.max(0, currentQuantity - quantity);
    //        super.increment(); // Increment the GCounter on removal
            items.put(itemName, newQuantity);
        }
    }

    private static void displayShopList(ShopListCRDT shopList) {
        System.out.println("\nShopping List Items:");
        for (Map.Entry<String, Integer> entry : shopList.getItems().entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
    

    public Map<String, Integer> getItems() {
        return new HashMap<>(items);
    }
}
