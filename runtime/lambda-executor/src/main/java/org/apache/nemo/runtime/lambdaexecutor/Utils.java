package org.apache.nemo.runtime.lambdaexecutor;

import javax.xml.bind.DatatypeConverter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {

    /**
     * Encode the given string with Base64
     * @param data the string to encode
     * @return the encoded string
     * @throws UnsupportedEncodingException
     */
    public static String base64Encode(String data) throws UnsupportedEncodingException {
        return DatatypeConverter.printBase64Binary(data.getBytes("UTF-8"));
    }

    /**
     * Compute the SHA-256 hash of the given byte array
     * @param data the byte array to hash
     * @return the hashed byte array
     * @throws NoSuchAlgorithmException
     */
    public static byte[] sha256Hash(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        return messageDigest.digest(data);
    }

    /**
     * Convert a byte array to its hex-string
     * @param data the byte array to convert
     * @return the hex-string of the byte array
     */
    public static String hexStringify(byte[] data) {
        StringBuilder stringBuilder = new StringBuilder();
        for (byte singleByte : data) {
            stringBuilder.append(Integer.toString((singleByte & 0xff) + 0x100, 16).substring(1));
        }

        return stringBuilder.toString();
    }

}
