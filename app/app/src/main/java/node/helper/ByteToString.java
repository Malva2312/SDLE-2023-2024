package node.helper;

import java.util.Base64;

public class ByteToString {
    public static String encodeToString(byte[] binaryData) {
        // Ensure that numberOfBytesToEncode is not greater than the length of binaryData
        int actualBytesToEncode = binaryData.length;
    
        // Create a new array with the correct length
        byte[] dataToEncode = new byte[actualBytesToEncode];
    
        // Copy the specified number of bytes from binaryData to dataToEncode
        System.arraycopy(binaryData, 0, dataToEncode, 0, actualBytesToEncode);
    
        // Encode the data to Base64
        return Base64.getEncoder().encodeToString(dataToEncode);
    }
    public static byte[] decodeFromString(String encodedString) {
        return Base64.getDecoder().decode(encodedString);
    }
}