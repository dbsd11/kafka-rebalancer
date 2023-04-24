package group.bison.kafka.rebalancer.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.http2.impl.nio.ProtocolNegotiationException;
import org.apache.hc.core5.pool.PoolConcurrencyPolicy;
import org.apache.hc.core5.pool.PoolReusePolicy;
import org.apache.hc.core5.reactor.IOReactorStatus;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpServerErrorException;


public class HttpUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);

    /**
     * http1 client connection manager
     */
    private static volatile Map<String, HttpClientConnectionManager> httpClientConnectionManagerMap = new ConcurrentHashMap<>();

    /**
     * 初始化http2 client
     */
    public static boolean supportHttp2 = true;
    private static volatile List<CloseableHttpAsyncClient> asyncHttp2ClientList = new ArrayList<>(2);
    static {
        // check if support http2
        try {
            CloseableHttpAsyncClient http2AsyncClient = initHttp2Client();
            SimpleHttpRequest request = new SimpleHttpRequest(Method.GET.name(),"https://www.taobao.com:443");
            http2AsyncClient.execute(request, null).get();
        } catch (Exception e) {
            LOGGER.info("detected not support http2", e);
            supportHttp2 = false;
        }

        for (int i = 0; i < 2; i++) {
            asyncHttp2ClientList.add(initHttp2Client());
        }
    }

    public static String httpsPost(Map<String, String> heads, String receipt, String url) {
        HttpClientConnectionManager httpClientConnectionManager = null;
        try{
            URI uri = new URI(url);
            String host = uri.getHost();
            if(!httpClientConnectionManagerMap.containsKey(host)) {
                httpClientConnectionManager = httpClientConnectionManagerMap.putIfAbsent(host, createConnectionManager(host));
            } else {
                httpClientConnectionManager = httpClientConnectionManagerMap.get(httpClientConnectionManager);
            }
        } catch (Exception e) {
            LOGGER.error("failed to get connection manager {}", e.getMessage());
        }
        
        String responseBody = null;
        try (CloseableHttpClient httpclient = org.apache.hc.client5.http.impl.classic.HttpClients.custom().setConnectionManager(httpClientConnectionManager).disableAutomaticRetries().evictIdleConnections(TimeValue.ofSeconds(600)).build()) {
            URI uri = new URI(url);
            HttpPost httpPost = new HttpPost(uri);
            if (!CollectionUtils.isEmpty(heads)) {
                heads.entrySet().forEach(header -> {
                    httpPost.addHeader(header.getKey(), header.getValue());
                });
            } else {
                httpPost.addHeader("Content-Type", "application/json");
                httpPost.addHeader("Proxy-Connection", "Keep-Alive");
            }
            httpPost.setEntity(new StringEntity(receipt));
            CloseableHttpResponse response = httpclient.execute(httpPost);
            BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            responseBody = br.lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            LOGGER.error("https post error", e);
        }
        return responseBody;
    }

    /*
     * 使用http2.0进行请求
     */
    public static String http2RequestWithPool(String method, String url, Map<String, String> headers, String receipt) {
        if(CollectionUtils.isEmpty(asyncHttp2ClientList)) {
            return null;
        }
        String requestId = MDC.get("requestId");
        URI uri = null;
        AtomicReference<String> responseAtom = new AtomicReference<>();
        try {
            Integer randClientIndex = ((int)(Math.random() * asyncHttp2ClientList.size())) % asyncHttp2ClientList.size();
            CloseableHttpAsyncClient client = asyncHttp2ClientList.get(randClientIndex);
            if (client.getStatus() != IOReactorStatus.ACTIVE) {
                synchronized(client) {
                    if(asyncHttp2ClientList.get(randClientIndex) == client) {
                        asyncHttp2ClientList.set(randClientIndex, initHttp2Client());
                        try {
                            client.close();
                        } catch (Exception e) {
                        }
                    }
                    client = asyncHttp2ClientList.get(randClientIndex);
                }
            }
            uri = new URI(url);
            SimpleHttpRequest httpRequest = new SimpleHttpRequest(Method.normalizedValueOf(method), uri);
            ContentType contentType = ContentType.APPLICATION_JSON;
            if(!CollectionUtils.isEmpty(headers)) {
                for(Map.Entry<String, String> headerEntry : headers.entrySet()) {
                    httpRequest.setHeader(headerEntry.getKey(), headerEntry.getValue());
                    if(headerEntry.getKey().equalsIgnoreCase("content-type")) {
                        contentType = ContentType.create(headerEntry.getValue(), StandardCharsets.UTF_8);
                    }
                }
            }
            if(StringUtils.isNotEmpty(receipt)) {
                httpRequest.setBody(receipt, contentType);
            }

            Future<SimpleHttpResponse> future = client.execute(httpRequest, new FutureCallback<SimpleHttpResponse>() {
                @Override
                public void completed(SimpleHttpResponse result) {
                    if(StringUtils.isNotEmpty(requestId)) {
                        MDC.put("requestId", requestId);
                    }
                    String responseStr = result.getBodyText();
                    responseAtom.set(responseStr);
                    if (result.getCode() >= 400) {
                        LOGGER.error("http2PostWithPool error! uri={} code={} reponse={}", url, result.getCode(), responseStr);
                        throw new HttpServerErrorException(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR, responseStr);
                    }
                }

                @Override
                public void failed(Exception ex) {
                    if(StringUtils.isNotEmpty(requestId)) {
                        MDC.put("requestId", requestId);
                    }
                    LOGGER.error("http2PostWithPool error! uri={}", url, ex);
                }

                @Override
                public void cancelled() {     
                    if(StringUtils.isNotEmpty(requestId)) {
                        MDC.put("requestId", requestId);
                    }
                    LOGGER.warn("request cancelled {}", url);               
                }
            });
            try{
                future.get(5, TimeUnit.SECONDS);
            }catch(Exception e) {
            }
        } catch (Exception e) {
            LOGGER.error("http2PostWithPool error! uri={}", uri !=null ? uri.getHost() : null, e);
        }
        return responseAtom.get();
    }

    static CloseableHttpAsyncClient initHttp2Client() {
        SSLContext sslContext = null;
        try {
            sslContext = SSLContexts.custom().loadTrustMaterial(TrustAllStrategy.INSTANCE).build();
        } catch (Exception e) {
            sslContext = SSLContexts.createDefault();
        }

        CloseableHttpAsyncClient asyncHttpClient = HttpAsyncClients.customHttp2()
                .setTlsStrategy(ClientTlsStrategyBuilder.create()
                        .setTlsVersions(TLS.V_1_0, TLS.V_1_1, TLS.V_1_2)
                        .setSslContext(sslContext)
                        .build())
                .setRetryStrategy(new DefaultHttpRequestRetryStrategy() {
                    @Override
                    public boolean retryRequest(HttpRequest request, IOException exception, int execCount,
                            HttpContext context) {
                        return exception instanceof ProtocolNegotiationException;
                    }

                    @Override
                    public boolean retryRequest(org.apache.hc.core5.http.HttpResponse response, int execCount,
                            HttpContext context) {
                        return false;
                    }
                })
                .setDefaultRequestConfig(org.apache.hc.client5.http.config.RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5))
                        .setConnectTimeout(Timeout.ofSeconds(600))
                        .setConnectionKeepAlive(TimeValue.ofSeconds(3600))
                        .setResponseTimeout(Timeout.ofSeconds(5)).setContentCompressionEnabled(false).build())
                .evictIdleConnections(TimeValue.ofSeconds(3600))
                .build();
        asyncHttpClient.start();
        return asyncHttpClient;
    }

    static HttpClientConnectionManager createConnectionManager(String host)
            throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
        // Trust standard CA and those trusted by our custom strategy
        final SSLContext sslcontext = SSLContexts.custom()
                .loadTrustMaterial((chain, authType) -> {
                    // CN=*.twilio.com, O="Twilio, Inc.", L=San Francisco, ST=California, C=US
//                    String name = chain[0].getSubjectDN().getName();
                    return true;
                })
                .build();
        // Allow TLSv1.2 protocol only
        final org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactoryBuilder.create()
                .setSslContext(sslcontext)
                .setTlsVersions(TLS.V_1_2)
                .build();
        final HttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                .setSSLSocketFactory(sslSocketFactory)
                .setConnPoolPolicy(PoolReusePolicy.FIFO)
                .setConnectionTimeToLive(TimeValue.ofSeconds(600))
                .setMaxConnPerRoute(100)
                .setMaxConnTotal(1000)
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
                .setDefaultSocketConfig(SocketConfig.custom().setSoKeepAlive(true).setSoTimeout(Timeout.ofSeconds(600)).build())
                .build();
        return cm;
    }
}