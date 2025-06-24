package MicroChallenger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class MicroChallengerClient {
    private static final String BASE_URL = "http://micro-challenger:8866/api";
    private final HttpClient client;
    private final ObjectMapper msgpack;

    public MicroChallengerClient() {
        this.client = HttpClient.newHttpClient();
        this.msgpack  = new ObjectMapper(new MessagePackFactory());
    }

    public String createAndStartBench(boolean test, int maxBatches) throws IOException, InterruptedException {
        String json = String.format("{ \"apitoken\": \"sabd\", \"name\": \"flink\", \"test\": %b, \"max_batches\": %d }", test, maxBatches);
        HttpRequest create = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/create"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        String benchId = trimQuotes(client.send(create, HttpResponse.BodyHandlers.ofString()).body());

        HttpRequest start = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/start/" + benchId))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        client.send(start, HttpResponse.BodyHandlers.ofString());

        return benchId;
    }

    public byte[] nextBatch(String benchId) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/next_batch/" + benchId))
                .GET()
                .build();

        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

        if (response.statusCode() == 404) return null;
        if (response.statusCode() != 200) throw new RuntimeException("Error: " + response.statusCode());

        return response.body();
    }

    public String sendResult(int worker, String benchId, TileClusterResult result)
            throws IOException, InterruptedException {
            // Serializzo il risultato in MessagePack
            byte[] body = msgpack.writeValueAsBytes(result);

            // Costruisco l'URL
            String url = String.format("%s/result/%d/%s/%d",
                    BASE_URL,
                    worker,
                    Objects.requireNonNull(benchId),
                    result.batch_id
            );

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/msgpack")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();

            // 3) invio e controllo status
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) {
                throw new RuntimeException(
                        "Errore invio result: HTTP " + resp.statusCode() +
                                " -> " + new String(resp.body())
                );
            }
            return new String(resp.body(), StandardCharsets.UTF_8);
    }

    private String trimQuotes(String raw) {
        return raw.trim().replaceAll("^\"|\"$", "");
    }

    public String endBench(String benchId) throws IOException, InterruptedException {

        //Costruisco l'url
        String url = String.format("%s/end/%s",
                BASE_URL,
                Objects.requireNonNull(benchId)
        );

        //Costruisci la request
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        //Esegui la chiamata
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

        // 3) Controlla lo status
        if (resp.statusCode() != 200) {
            throw new RuntimeException("Errore endBench: HTTP " + resp.statusCode()
                    + " -> " + resp.body());
        }

        // 4) Restituisci il contenuto testuale della risposta
        return resp.body();
    }


}

