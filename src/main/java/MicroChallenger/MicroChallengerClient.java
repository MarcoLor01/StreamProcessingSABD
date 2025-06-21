package MicroChallenger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
public class MicroChallengerClient {
    private static final String BASE_URL = "http://micro-challenger:8866/api";
    private final HttpClient client;

    public MicroChallengerClient() {
        this.client = HttpClient.newHttpClient();
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
    public void sendResult() {
        //TODO
    }

    private String trimQuotes(String raw) {
        return raw.trim().replaceAll("^\"|\"$", "");
    }
}

