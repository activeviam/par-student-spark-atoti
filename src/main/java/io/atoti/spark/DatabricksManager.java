package io.atoti.spark;

import io.github.cdimascio.dotenv.Dotenv;
import org.json.JSONObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static java.time.temporal.ChronoUnit.SECONDS;


public class DatabricksManager {
  static Dotenv dotenv = Dotenv.load();
  static private final String clusterId = dotenv.get("clusterId");
  static private final String token = dotenv.get("token");
  static private final String clusterUrl = dotenv.get("clusterUrl");
  static private final int timeOut = 10;

  private static HttpRequest buildHttpQuery(String endPoint, String method, JSONObject body) throws Exception {
    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(new URI(clusterUrl + endPoint));

    switch (method) {
      case "POST" -> requestBuilder = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body.toString()));
      case "GET" -> requestBuilder = requestBuilder.GET();
      default -> throw new Exception(method + " is not a valid method. Available methods are : POST, GET");
    }

    return requestBuilder
            .header("Content-Type","application/json")
            .header("Authorization", "Bearer " + token)
            .timeout(Duration.of(timeOut, SECONDS))
            .build();
  }

  private static JSONObject buildHttpResponse(HttpResponse<String> response) throws Exception {
    int status = response.statusCode();

    if (status > 299) {
      throw new Exception(response.body());
    } else {
      return new JSONObject(response.body());
    }
  }

  private static JSONObject apiSyncCall(String endPoint, String method, JSONObject body) throws Exception {
    HttpRequest request = DatabricksManager.buildHttpQuery(endPoint, method, body);

    HttpResponse<String> response = HttpClient
            .newBuilder()
            .build()
            .send(request, HttpResponse.BodyHandlers.ofString());

    return DatabricksManager.buildHttpResponse(response);
  }

  private static CompletableFuture<JSONObject> apiAsyncCall(String endPoint, String method, JSONObject body) throws Exception {
    HttpRequest request = DatabricksManager.buildHttpQuery(endPoint, method, body);

     return HttpClient
             .newBuilder()
             .build()
             .sendAsync(request, HttpResponse.BodyHandlers.ofString())
             .thenApply((response) -> {
               try {
                 return DatabricksManager.buildHttpResponse(response);
               } catch (Exception e) {
                 e.printStackTrace();
                 return null;
               }
             });
  }

  public static void resize(int workerNumber, boolean wait) throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);
    body.put("num_workers", workerNumber);

    JSONObject response = DatabricksManager.apiSyncCall(
            "api/2.0/clusters/resize",
            "POST",
            body);
  }

  public static CompletableFuture<JSONObject> resizeAsync(int workerNumber, boolean wait) throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);
    body.put("num_workers", workerNumber);

    return DatabricksManager.apiAsyncCall(
            "api/2.0/clusters/resize",
            "POST",
            body);
  }

  public static JSONObject state() throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);

    return DatabricksManager.apiSyncCall(
            "api/2.0/clusters/get",
            "POST",
            body);
  }

  public static CompletableFuture<JSONObject> stateAsync() throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);

    return DatabricksManager.apiAsyncCall(
            "api/2.0/clusters/get",
            "POST",
            body);
  }
}
