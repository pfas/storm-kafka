package util;

import okhttp3.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class AppUtil {


    public static ResponseBody doPost(String url, String json) throws IOException {
        OkHttpClient client = new OkHttpClient();
        final MediaType JSON = MediaType.get("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            return response.body();
        }
    }

    public static ResponseBody doGet(String url, Map<String, Object> headers, Map<String, Object> query) throws IOException {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .build();

        Request.Builder builder = new Request.Builder();
        HttpUrl.Builder urlBuilder = request.url().newBuilder();
        Headers.Builder headerBuilder = request.headers().newBuilder();

        // 装载请求头参数
        Iterator<Map.Entry<String, Object>> headerIterator = headers.entrySet().iterator();
        headerIterator.forEachRemaining(e -> {
            headerBuilder.add(e.getKey(), (String) e.getValue());
        });

        // 装载请求的参数
        Iterator<Map.Entry<String, Object>> queryIterator = query.entrySet().iterator();
        queryIterator.forEachRemaining(e -> {
            urlBuilder.addQueryParameter(e.getKey(), (String) e.getValue());
        });


        builder.url(urlBuilder.build()).headers(headerBuilder.build());

        try (Response response = client.newCall(builder.build()).execute()) {
            return response.body();
        }
    }
}
