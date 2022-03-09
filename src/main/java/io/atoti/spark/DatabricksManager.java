package io.atoti.spark;

import io.github.cdimascio.dotenv.Dotenv;
import okhttp3.*;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class DatabricksManager {
  static Dotenv dotenv = Dotenv.load();
  static private final String CLUSTER_ID = dotenv.get("clusterId");
  static private final String TOKEN = dotenv.get("token");
  static private final String CLUSTER_URL = dotenv.get("clusterUrl");
  static private final int TIME_OUT = 10;
  static private final int DELAY_BEFORE_STATE_CHECK = 1000;
  static private final int PERIOD_STATE_CHECK = 5000;
  static private final int MAX_STATE_CHECK = 60;
  static private final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private static Request buildHttpQuery(String endPoint, JSONObject body) {
    Request.Builder requestBuilder = new Request
            .Builder()
            .url(CLUSTER_URL + endPoint)
            .post(RequestBody.create(body.toString(), JSON));

    return requestBuilder
            .header("Content-Type","application/json")
            .addHeader("Authorization", "Bearer " + TOKEN)
            .build();
  }

  private static JSONObject buildHttpResponse(Response response) {
    if (response.isSuccessful()) {
      try {
        return new JSONObject(response.body().string());
      } catch (Exception e) {
        throw new RuntimeException(response.toString());
      }
    } else {
      throw new RuntimeException(response.toString());
    }
  }

  private static CompletableFuture<JSONObject> apiAsyncCall(String endPoint, JSONObject body) {
    OkHttpClient client = new OkHttpClient
            .Builder()
            .connectTimeout(TIME_OUT, TimeUnit.SECONDS)
            .writeTimeout(TIME_OUT, TimeUnit.SECONDS)
            .readTimeout(TIME_OUT, TimeUnit.SECONDS)
            .build();
    Request request = DatabricksManager.buildHttpQuery(endPoint, body);
    CompletableFuture<JSONObject> responseFuture = new CompletableFuture<>();

    client.newCall(request).enqueue(new Callback() {
      @Override public void onFailure(Call call, IOException e) {
        responseFuture.completeExceptionally(e);
        client.dispatcher().executorService().shutdown();
      }

      @Override public void onResponse(Call call, Response response) {
        try {
          responseFuture.complete(DatabricksManager.buildHttpResponse(response));
        } catch (Exception e) {
          responseFuture.completeExceptionally(e);
        }
        client.dispatcher().executorService().shutdown();
      }
    });

    return responseFuture;
  }

  private static CompletableFuture<JSONObject> checkState(String targetState, List<String> allowedState, JSONObject previousResponse) {
    CompletableFuture<JSONObject> ready = new CompletableFuture<>();

    Timer timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      int count = 0;

      @Override
      public void run() {
        count++;
        try {
          JSONObject response = DatabricksManager.state();
          if(response.get("state").toString().equals(targetState)) {
            ready.complete(previousResponse);
            timer.cancel();
          } else if(!allowedState.contains(response.get("state").toString())) {
            ready.completeExceptionally(new Exception("Cluster is in an illegal state: " + response.get("state").toString()));
            timer.cancel();
          }
        } catch (Exception e) {
          ready.completeExceptionally(e);
          timer.cancel();
        }

        if(count >= MAX_STATE_CHECK) {
          ready.completeExceptionally(new Exception("The cluster has not reached the " + targetState + " state after the allowed time"));
          timer.cancel();
        }
      }
    }, DELAY_BEFORE_STATE_CHECK, PERIOD_STATE_CHECK);

    return ready;
  }

  public static JSONObject resize(int workerNumber, boolean wait) {
    try {
      return DatabricksManager.resizeAsync(workerNumber, wait).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static CompletableFuture<JSONObject> resizeAsync(int workerNumber, boolean wait) {
    JSONObject body = new JSONObject();
    body.put("cluster_id", CLUSTER_ID);
    body.put("num_workers", workerNumber);

    CompletableFuture<JSONObject> response = DatabricksManager.apiAsyncCall(
            "api/2.0/clusters/resize",
            body);

    if(wait) {
      response = response.thenCompose(
              (r) -> DatabricksManager.checkState("RUNNING", Arrays.asList("RUNNING", "RESIZING"), r)
      );
    }

    return response;
  }

  public static JSONObject state() {
    try {
      return DatabricksManager.stateAsync().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static CompletableFuture<JSONObject> stateAsync() {
    JSONObject body = new JSONObject();
    body.put("cluster_id", CLUSTER_ID);

    return DatabricksManager.apiAsyncCall(
            "api/2.0/clusters/get",
            body);
  }
}
