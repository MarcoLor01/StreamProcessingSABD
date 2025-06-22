package MicroChallenger;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

    public void sendResult(int worker, String benchId, TileClusterResult result)
            throws IOException, InterruptedException {

        // 1) serializzo il risultato in MessagePack
        byte[] body = msgpack.writeValueAsBytes(result);

        // 2) costruisco l'URL
        String url = String.format("%s/result/%d/%s/%d",
                BASE_URL,
                worker,
                Objects.requireNonNull(benchId),
                result.batchId
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
    }

    private String trimQuotes(String raw) {
        return raw.trim().replaceAll("^\"|\"$", "");
    }
}

