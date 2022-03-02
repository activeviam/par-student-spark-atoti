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
  static private final String clusterId = dotenv.get("clusterId");
  static private final String token = dotenv.get("token");
  static private final String clusterUrl = dotenv.get("clusterUrl");
  static private final int timeOut = 10;
  static private final int delayBeforeStateCheck = 1000;
  static private final int periodStateCheck = 5000;
  static private final int maxStateCheck = 60;
  static private final MediaType JSON = MediaType.get("application/json; charset=utf-8");

  private static Request buildHttpQuery(String endPoint, JSONObject body) throws Exception {
    Request.Builder requestBuilder = new Request
            .Builder()
            .url(clusterUrl + endPoint)
            .post(RequestBody.create(body.toString(), JSON));

    return requestBuilder
            .header("Content-Type","application/json")
            .addHeader("Authorization", "Bearer " + token)
            .build();
  }

  private static JSONObject buildHttpResponse(Response response) throws Exception {
    if (response.isSuccessful()) {
      return new JSONObject(response.body().string());
    } else {
      throw new Exception(response.toString());
    }
  }

  private static JSONObject apiSyncCall(String endPoint, JSONObject body) throws Exception {
    OkHttpClient client = new OkHttpClient
            .Builder()
            .connectTimeout(timeOut, TimeUnit.SECONDS)
            .writeTimeout(timeOut, TimeUnit.SECONDS)
            .readTimeout(timeOut, TimeUnit.SECONDS)
            .build();
    Request request = DatabricksManager.buildHttpQuery(endPoint, body);

    return DatabricksManager.buildHttpResponse(client.newCall(request).execute());
  }

  private static CompletableFuture<JSONObject> apiAsyncCall(String endPoint, JSONObject body) throws Exception {
    OkHttpClient client = new OkHttpClient
            .Builder()
            .connectTimeout(timeOut, TimeUnit.SECONDS)
            .writeTimeout(timeOut, TimeUnit.SECONDS)
            .readTimeout(timeOut, TimeUnit.SECONDS)
            .build();
    Request request = DatabricksManager.buildHttpQuery(endPoint, body);
    CompletableFuture<JSONObject> responseFuture = new CompletableFuture<>();

    client.newCall(request).enqueue(new Callback() {
      @Override public void onFailure(Call call, IOException e) {
        e.printStackTrace();
        responseFuture.complete(null);
        client.dispatcher().executorService().shutdown();
      }

      @Override public void onResponse(Call call, Response response) {
        try {
          responseFuture.complete(DatabricksManager.buildHttpResponse(response));
        } catch (Exception e) {
          e.printStackTrace();
        }
        client.dispatcher().executorService().shutdown();
      }
    });

    return responseFuture;
  }

  private static CompletableFuture<Boolean> checkState(String targetState, List<String> allowedState) {
    CompletableFuture<Boolean> ready = new CompletableFuture<>();

    Timer timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      int count = 0;

      @Override
      public void run() {
        count++;
        try {
          JSONObject response = DatabricksManager.state();
          if(response.get("state").toString().equals(targetState)) {
            ready.complete(true);
            timer.cancel();
          } else if(!allowedState.contains(response.get("state").toString())) {
            ready.complete(false);
            timer.cancel();
          }
        } catch (Exception e) {
          e.printStackTrace();
          ready.complete(false);
          timer.cancel();
        }

        if(count >= maxStateCheck) {
          ready.complete(false);
          timer.cancel();
        }
      }
    }, delayBeforeStateCheck,periodStateCheck);

    return ready;
  }

  public static void resize(int workerNumber, boolean wait) throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);
    body.put("num_workers", workerNumber);

    DatabricksManager.apiSyncCall(
            "api/2.0/clusters/resize",
            body);

    if(wait) {
      System.out.println("Waiting for the end of resizing : this action can take up to " + maxStateCheck*periodStateCheck/1000 + "s");
      boolean result = DatabricksManager.checkState("RUNNING", Arrays.asList("RUNNING", "RESIZING")).get();

      if (!result) {
        throw new Exception("Error while providing new workers to the cluster");
      }
      System.out.println("The cluster has been resized successfully!");
    }
  }

  public static CompletableFuture<JSONObject> resizeAsync(int workerNumber, boolean wait) throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);
    body.put("num_workers", workerNumber);

    CompletableFuture<JSONObject> response = DatabricksManager.apiAsyncCall(
            "api/2.0/clusters/resize",
            body);

    if(wait) {
      response = response.thenApply((r) -> {
        try {
          boolean result = DatabricksManager.checkState("RUNNING", Arrays.asList("RUNNING", "RESIZING")).get();
          if (!result) {
            throw new Exception("Error while providing new workers to the cluster");
          }
          return r;
        } catch (Exception e) {
          e.printStackTrace();
          return null;
        }
      });
    }

    return response;
  }

  public static JSONObject state() throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);

    return DatabricksManager.apiSyncCall(
            "api/2.0/clusters/get",
            body);
  }

  public static CompletableFuture<JSONObject> stateAsync() throws Exception {
    JSONObject body = new JSONObject();
    body.put("cluster_id", clusterId);

    return DatabricksManager.apiAsyncCall(
            "api/2.0/clusters/get",
            body);
  }
}
