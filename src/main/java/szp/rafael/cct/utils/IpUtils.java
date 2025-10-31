package szp.rafael.cct.utils;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public final class IpUtils {

    private IpUtils() {}

    /**
     * Retorna true se ambas strings representam IPs públicos e idênticos.
     * Lança IllegalArgumentException se algum IP for inválido ou não for público.
     */
    public static boolean areSamePublicIP(String ip1, String ip2) {
        InetAddress a = parseInetAddress(ip1);
        InetAddress b = parseInetAddress(ip2);

        if (!isPublic(a)) {
            throw new IllegalArgumentException("IP não é público: " + ip1);
        }
        if (!isPublic(b)) {
            throw new IllegalArgumentException("IP não é público: " + ip2);
        }

        return addressesEqual(a, b);
    }

    /**
     * Retorna true se os dois IPs (válidos) representam o mesmo endereço (mesmo valor).
     * Não valida público/privado.
     */
    public static boolean addressesEqual(String ip1, String ip2) {
        InetAddress a = parseInetAddress(ip1);
        InetAddress b = parseInetAddress(ip2);
        return addressesEqual(a, b);
    }

    private static boolean addressesEqual(InetAddress a, InetAddress b) {
        byte[] ba = normalizedBytes(a);
        byte[] bb = normalizedBytes(b);
        return Arrays.equals(ba, bb);
    }

    public static InetAddress parseInetAddress(String ip) {
        try {
            return InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Endereço inválido: " + ip, e);
        }
    }

    /**
     * Retorna os bytes normalizados do endereço:
     * - IPv4 -> 4 bytes
     * - IPv6 -> 16 bytes
     * - IPv4-mapped IPv6 (::ffff:a.b.c.d) -> normalize para 4 bytes (IPv4)
     */
    private static byte[] normalizedBytes(InetAddress addr) {
        byte[] raw = addr.getAddress();

        // Detect IPv4-mapped IPv6: ::ffff:a.b.c.d  -> last 4 bytes are IPv4
        if (raw.length == 16 && isIPv4Mapped(raw)) {
            return Arrays.copyOfRange(raw, 12, 16); // normalize para IPv4 (4 bytes)
        }

        return raw;
    }

    private static boolean isIPv4Mapped(byte[] v6) {
        // ::ffff:a.b.c.d -> first 10 bytes zero, next 2 bytes 0xff,0xff
        for (int i = 0; i < 10; i++) {
            if (v6[i] != 0) return false;
        }
        return (v6[10] == (byte) 0xff && v6[11] == (byte) 0xff);
    }

    /**
     * Testa se um InetAddress é considerado "público".
     * Exclui: loopback, link-local, multicast, unspecified, site-local (RFC1918),
     * IPv4 carrier-grade NAT (100.64.0.0/10), IPv4 "0/8", IPv4 broadcast, IPv6 ULA (fc00::/7), etc.
     *
     * Nota: não tenta cobrir absolutamente todas as faixas rsvd possíveis, mas cobre as mais relevantes.
     */
    public static boolean isPublic(InetAddress addr) {
        if (addr.isAnyLocalAddress() || addr.isLoopbackAddress() ||
                addr.isLinkLocalAddress() || addr.isMulticastAddress() ||
                addr.isSiteLocalAddress()) {
            return false;
        }

        byte[] raw = addr.getAddress();

        if (raw.length == 4) { // IPv4
            int a = Byte.toUnsignedInt(raw[0]);
            int b = Byte.toUnsignedInt(raw[1]);

            // 10.0.0.0/8
            if (a == 10) return false;
            // 172.16.0.0/12 -> 172.16.0.0 - 172.31.255.255
            if (a == 172 && (b >= 16 && b <= 31)) return false;
            // 192.168.0.0/16
            if (a == 192 && b == 168) return false;
            // 127.0.0.0/8 loopback (already caught by isLoopbackAddress)
            // 169.254.0.0/16 link-local (caught by isLinkLocalAddress)
            // 100.64.0.0/10 carrier-grade NAT
            if (a == 100 && (b >= 64 && b <= 127)) return false;
            // 0.0.0.0/8 (invalid/unspecified)
            if (a == 0) return false;
            // 255.255.255.255 broadcast (rare) -> treat como não público
            if (a == 255 && Byte.toUnsignedInt(raw[1]) == 255
                    && Byte.toUnsignedInt(raw[2]) == 255 && Byte.toUnsignedInt(raw[3]) == 255) {
                return false;
            }
            // 192.0.0.0/24 (IANA / special) - optional to block, leaving public for now
            // Outros blocos reservados podem ser adicionados conforme necessário
            return true;
        } else { // IPv6 (16 bytes)
            // Unique Local Addresses (fc00::/7) -> não público
            int first = Byte.toUnsignedInt(raw[0]);
            if ((first & 0xFE) == 0xFC) { // fc00::/7 => first 7 bits 1111110x (0xFC/0xFD)
                return false;
            }
            // IPv4-mapped IPv6 handled by normalizedBytes when comparing,
            // but here we should check the embedded IPv4 if needed.
            if (addr instanceof Inet6Address) {
                Inet6Address a6 = (Inet6Address) addr;
                // link-local (::fe80::/10) and others already caught by isLinkLocalAddress
            }
            return true;
        }
    }

    // ---------- Exemplo de uso ----------
    public static void main(String[] args) {
        // exemplos
        try {
            System.out.println("192.0.2.1 == 192.0.2.1 ? " +
                    addressesEqual("192.0.2.1", "192.0.2.1"));

            System.out.println("::ffff:192.0.2.1 == 192.0.2.1 ? " +
                    addressesEqual("::ffff:192.0.2.1", "192.0.2.1"));

            System.out.println("10.0.0.1 é público? " + isPublic(parseInetAddress("10.0.0.1")));
            System.out.println("8.8.8.8 é público? " + isPublic(parseInetAddress("8.8.8.8")));

            // checar se dois IPs públicos são idênticos
            System.out.println("areSamePublicIP(8.8.8.8, 8.8.8.8) => " +
                    areSamePublicIP("8.8.8.8", "8.8.8.8"));

            // este lançará IllegalArgumentException porque 10.0.0.1 não é público
            System.out.println("areSamePublicIP(8.8.8.8, 10.0.0.1) => " +
                    areSamePublicIP("8.8.8.8", "10.0.0.1"));
        } catch (IllegalArgumentException ex) {
            System.err.println("Erro: " + ex.getMessage());
        }
    }
}
