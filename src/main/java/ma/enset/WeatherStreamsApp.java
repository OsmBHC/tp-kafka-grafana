package ma.enset;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import java.util.Properties;

public class WeatherStreamsApp {

    // Métriques Prometheus pour température moyenne
    private static final Gauge temperatureGauge = Gauge.build()
            .name("weather_average_temperature")
            .help("Température moyenne en Fahrenheit par station")
            .labelNames("station")
            .register();

    // Métriques Prometheus pour humidité moyenne
    private static final Gauge humidityGauge = Gauge.build()
            .name("weather_average_humidity")
            .help("Humidité moyenne en pourcentage par station")
            .labelNames("station")
            .register();

    public static void main(String[] args) throws IOException {
        // ========= Configuration Kafka Streams =========
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("application.id", "weather-streams-app");
        prop.put("default.key.serde", Serdes.String().getClass().getName());
        prop.put("default.value.serde", Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Lire les données météorologiques depuis le topic 'weather-data'
        KStream<String, String> weatherData = builder.stream("weather-data");

        // Filtrer les données invalides (null ou vides)
        KStream<String, String> validWeatherData = weatherData.filter((k, v) ->
                v != null && !v.isEmpty() && v.split(",").length == 3
        );

        // 2. Filtrer les données de température élevée (> 30°C)
        KStream<String, String> highTempData = validWeatherData.filter((k, v) -> {
            String[] parts = v.split(",");
            double temperature = Double.parseDouble(parts[1]);
            return temperature > 30.0;
        });

        // 3. Convertir les températures en Fahrenheit
        KStream<String, String> fahrenheitData = highTempData.mapValues(value -> {
            String[] parts = value.split(",");
            String station = parts[0];
            double celsius = Double.parseDouble(parts[1]);
            double humidity = Double.parseDouble(parts[2]);

            // Conversion Celsius vers Fahrenheit
            double fahrenheit = (celsius * 9.0 / 5.0) + 32.0;

            return station + "," + fahrenheit + "," + humidity;
        });

        // 4. Grouper les données par station
        KGroupedStream<String, String> groupedByStation = fahrenheitData.groupBy(
                (key, value) -> value.split(",")[0]
        );

        // Calculer les moyennes de température et d'humidité par station
        KTable<String, String> stationAverages = groupedByStation.aggregate(
                // Initializer: count, sumTemp, sumHumidity
                () -> "0,0.0,0.0",
                // Aggregator
                (station, value, aggregate) -> {
                    String[] parts = value.split(",");
                    double temperature = Double.parseDouble(parts[1]);
                    double humidity = Double.parseDouble(parts[2]);

                    String[] aggParts = aggregate.split(",");
                    int count = Integer.parseInt(aggParts[0]);
                    double sumTemp = Double.parseDouble(aggParts[1]);
                    double sumHumidity = Double.parseDouble(aggParts[2]);

                    // Mise à jour des sommes
                    count++;
                    sumTemp += temperature;
                    sumHumidity += humidity;

                    // Calcul des moyennes
                    double avgTemp = sumTemp / count;
                    double avgHumidity = sumHumidity / count;

                    // Mise à jour des métriques Prometheus
                    temperatureGauge.labels(station).set(avgTemp);
                    humidityGauge.labels(station).set(avgHumidity);

                    // Affichage console
                    System.out.println(station + " : Température Moyenne = " +
                            String.format("%.2f", avgTemp) + "°F, Humidité Moyenne = " +
                            String.format("%.2f", avgHumidity) + "%");

                    return count + "," + sumTemp + "," + sumHumidity;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // 5. Écrire les résultats dans le topic 'station-averages'
        stationAverages.toStream().mapValues((station, aggregate) -> {
            String[] parts = aggregate.split(",");
            int count = Integer.parseInt(parts[0]);
            double sumTemp = Double.parseDouble(parts[1]);
            double sumHumidity = Double.parseDouble(parts[2]);

            double avgTemp = sumTemp / count;
            double avgHumidity = sumHumidity / count;

            return station + " : Température Moyenne = " +
                    String.format("%.2f", avgTemp) + "°F, Humidité Moyenne = " +
                    String.format("%.2f", avgHumidity) + "%";
        }).to("station-averages");

        // ========= Démarrer le serveur HTTP Prometheus (/metrics) =========
        DefaultExports.initialize(); // métriques JVM
        HTTPServer metricsServer = new HTTPServer(1234); // expose http://localhost:1234/metrics
        System.out.println("Serveur Prometheus démarré sur http://localhost:1234/metrics");

        // ========= Lancement Kafka Streams =========
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), prop);
        kafkaStreams.start();
        System.out.println("Application Kafka Streams démarrée...");

        // Hook pour arrêt propre
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Arrêt de l'application...");
            kafkaStreams.close();
            metricsServer.stop();
            System.out.println("Application arrêtée proprement.");
        }));
    }
}