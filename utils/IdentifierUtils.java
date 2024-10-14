package utils;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public final class IdentifierUtils {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int RADIX = 16;

    private IdentifierUtils() {
    }

    public static String generateRandomIdentifier(int length) {
        final StringBuilder identifier = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            identifier.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
        }
        return identifier.toString();
    }

    public static byte[] toByteArray(String id) {
        return id.getBytes(StandardCharsets.UTF_8);
    }

    public static String toString(byte[] id) {
        return new String(id, StandardCharsets.UTF_8);
    }

    public static String toHex(byte[] id) {
        return HashProducer.toNumberFormat(id).toString(RADIX);
    }

    public static BigInteger toNumerical(byte[] id) {
        return HashProducer.toNumberFormat(id);
    }
}