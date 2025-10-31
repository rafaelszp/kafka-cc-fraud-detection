package szp.rafael.cct.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadLocalRandom;

public final class InstantUtils {

    private InstantUtils() {}

    /**
     * Calcula a diferença entre dois Instants.
     * Retorna um Duration representando a diferença (sempre positiva se preferir).
     *
     * @param start instante inicial
     * @param end instante final
     * @return duração (end - start)
     */
    public static Duration calcularDiferenca(Instant start, Instant end) {
        return Duration.between(start, end);
    }

    /**
     * Calcula a diferença absoluta entre dois Instants (ordem não importa).
     */
    public static Duration calcularDiferencaAbsoluta(Instant a, Instant b) {
        Duration d = Duration.between(a, b);
        return d.isNegative() ? d.negated() : d;
    }


    /**
     * Exemplo de uso.
     */
    public static void main(String[] args) throws InterruptedException {
        Instant inicio = Instant.now();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Instant fim = Instant.now().plus(random.nextInt(1000,36000),ChronoUnit.SECONDS);

        Duration diferenca = calcularDiferenca(inicio, fim);

        System.out.println("Diferença bruta: " + diferenca);
        System.out.println("Segundos: " + diferenca.getSeconds());
        System.out.println("Minutos: " + diferenca.getSeconds()/60);
        System.out.println("Milissegundos (aprox): " + diferenca.toMillis());

        Duration abs = calcularDiferencaAbsoluta(fim, inicio);
        System.out.println("Diferença absoluta: " + abs);

        // você também pode decompor a diferença:
        long segundos = diferenca.getSeconds();
        long nanos = diferenca.getNano();
        System.out.printf("Δ = %d segundos e %d nanos%n", segundos, nanos);
    }
}

