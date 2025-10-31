package szp.rafael.cct.utils;

public class GeoUtils {
    private static final double RAIO_TERRA_KM = 6371.0; // Raio médio da Terra em km

    /**
     * Calcula a distância entre dois pontos geográficos (em km)
     * usando a fórmula de Haversine.
     *
     * @param lat1 latitude do ponto 1
     * @param lon1 longitude do ponto 1
     * @param lat2 latitude do ponto 2
     * @param lon2 longitude do ponto 2
     * @return distância em quilômetros
     */
    public static double calcularDistancia(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return RAIO_TERRA_KM * c;
    }

    public static void main(String[] args) {
        // Exemplo: São Paulo (-23.5505, -46.6333) → Rio de Janeiro (-22.9068, -43.1729)
        double distancia = calcularDistancia(-23.5505, -46.6333, -22.9068, -43.1729);

        System.out.printf("Distância SP → RJ: %.2f km%n", distancia);
    }
}
