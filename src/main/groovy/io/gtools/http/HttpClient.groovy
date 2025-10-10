package io.gtools.http

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.TupleConstructor
import groovy.transform.stc.ClosureParams
import io.gio.Result
import groovy.util.logging.Slf4j

import java.net.http.HttpRequest
import java.util.concurrent.CompletableFuture
import java.net.http.HttpRequest as JavaHttpRequest
import java.net.http.HttpResponse as JavaHttpResponse
import java.net.http.HttpClient as JavaHttpClient

import static io.gio.Result.tryOf

trait HttpAuthMethod {}

@TupleConstructor
class HttpAuthBasic implements HttpAuthMethod {
    String username
    String password

    String getEncoded() {
        Base64.encoder.encodeToString("$username:$password".bytes)
    }
}

@TupleConstructor
class HttpAuthBearerToken implements HttpAuthMethod {
    String token
}

class HttpAuth {

    private HttpAuthMethod auth

    def basic(String username, String password){
        auth = new HttpAuthBasic(username, password)
    }

    def bearerToken(String token){
        auth = new HttpAuthBearerToken(token)
    }

    boolean hasAuth() { auth != null }

    HttpAuthMethod getMethod() { auth }

}

@Slf4j
class HttpClient {

    Object method = Method.GET
    /**
     * Content-Type do conteúdo recebido
     */
    Object contentType = ContentType.JSON
    /**
     * Content-Type do conteúdo enviado
     */
    Object requestContentType = ContentType.JSON
    Boolean asJson = false
    @Deprecated
    boolean throwEx
    boolean throwIfNotOk
    Object body
    String url
    boolean debug
    Map<String, Object> headers = [:]
    Closure success
    Closure error
    Closure executor
    Closure recover
    Closure failWith
    HttpResponse response
    JavaHttpResponse.BodyHandler bodyHandler
    HttpAuth auth = new HttpAuth()

    HttpClient(@DelegatesTo(HttpClient) Closure executor) {
        this.executor = executor
        this.executor.delegate = this
    }

    /**
     * Http client that encapsulate HTTPBuilder lógic.
     * The call is executed when verb method, like a <code>.get()</code> is invoked.
     * Never throw a exception, to this use <code>Try.throwOnFailure()</code>.
     * <code>throwIfNotOk</code> indicate that any response different of 200 will be throw and can
     * be catch by <code>Try</code>
     * The body follow HTTPBuilder conventions.
     * <pre>
     * html {
     *     url = 'http://api.com'
     *     // or 'GET', 'POST', etc.. Value is Optional, require only use with .exec()
     *     method = Method.GET
     *     headers = [:]
     *     body = payload
     *     // throw exception on failure
     *     throwIfNotOk = true
     *     // or 'application/json'. response content type, default is JSON
     *     contentType = ContentType.JSON
     *     // or 'application/json'. request content type type, default is JSON
     *     requestContentType = ContentType.JSON
     *     success = { HttpResponse resp ->
     *
     *     }
     *     error = { HttpResponse resp ->
     *
     *     }
     *
     *     recover = { HttpResponse resp ->
     *         return resp.statusCode == 404 // recover on 404
     *     }
     * }
     * .[get(), post(), put(), delete(), exec()]
     * .ifOk { HttpResponse resp }
     * .ifFailure { ex -> }
     * .toOption().map { HttpResponse resp }
     * </pre>
     * @param c http configs
     * @return Option<HttpResponse>
     */
    static HttpClient http(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c)
    }

    static Result<HttpResponse> doGet(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).get()
    }

    static CompletableFuture<HttpResponse> doGetAsync(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).getAsync()
    }

    static Result<HttpResponse> doPost(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).post()
    }

    static CompletableFuture<HttpResponse> doPostAsync(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).postAsync()
    }

    static Result<HttpResponse> doPut(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).put()
    }

    static CompletableFuture<HttpResponse> doPutAsync(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).putAsync()
    }

    static Result<HttpResponse> doDelete(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).delete()
    }

    static CompletableFuture<HttpResponse> doDeleteAsync(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).deleteAsync()
    }

    JavaHttpResponse.BodyHandler ofByteArray() {
        JavaHttpResponse.BodyHandlers.ofByteArray()
    }

    JavaHttpResponse.BodyHandler ofInputStream() {
        JavaHttpResponse.BodyHandlers.ofInputStream()
    }

    JavaHttpResponse.BodyHandler ofString() {
        JavaHttpResponse.BodyHandlers.ofString()
    }

    Result<HttpResponse> post() { request(Method.POST) }

    CompletableFuture<HttpResponse> postAsync() { requestAsync(Method.POST) }

    Result<HttpResponse> get() { request(Method.GET) }

    CompletableFuture<HttpResponse> getAsync() { requestAsync(Method.GET) }

    Result<HttpResponse> put() { request(Method.PUT) }

    CompletableFuture<HttpResponse> putAsync() { requestAsync(Method.PUT) }

    Result<HttpResponse> delete() { request(Method.DELETE) }

    CompletableFuture<HttpResponse> deleteAsync() { requestAsync(Method.DELETE) }

    Result<HttpResponse> exec() { request() }

    CompletableFuture<HttpResponse> execAsync() { requestAsync() }

    Result<HttpResponse> request(Object method = null) {
        if (method) this.method = method
        tryOf {
            this.execute()
            this.response
        }
    }

    CompletableFuture<HttpResponse> requestAsync(Object method = null) {
        if (method) this.method = method
        executeAsync()
    }


    private void execute() {
        this.executor.call()

        debugRequest()

        final client = JavaHttpClient.newHttpClient()
        final builder = createRequestBuilder()
        final request = builder.build()
        final resp = client
            .send(request, JavaHttpResponse.BodyHandlers.ofString())

        if(resp.statusCode() != 200){
            handleError(resp)
        } else {
            handleSuccess(resp)
        }
    }

    private CompletableFuture<HttpResponse> executeAsync() {
        this.executor.call()

        debugRequest()

        final future = new CompletableFuture<HttpResponse>()

        final client = JavaHttpClient.newHttpClient()
        final builder = createRequestBuilder()
        final request = builder.build()
        final handler = bodyHandler ?:
            contentType == ContentType.PDF ? JavaHttpResponse.BodyHandlers.ofByteArray() : JavaHttpResponse.BodyHandlers.ofString()

        client
            .sendAsync(request, handler)
            .thenApply {resp ->

                try {
                    if (resp.statusCode() != 200) {
                        handleError(resp)
                    } else {
                        handleSuccess(resp)
                    }
                    future.complete(response)

                }catch (Throwable err){
                    future.completeExceptionally(err)
                }

            } exceptionally {err ->
                future.completeExceptionally(err)
            }

        future

    }

    private void fillResponse(JavaHttpResponse response) {
        def rawBody = response.body()
        def body = null

        if(contentType == ContentType.JSON && bodyHandler == null){
            body = new JsonSlurper().parseText(response.body())
        }

        this.response = new HttpResponse(
            rawResp: response,
            rawBody: rawBody,
            body: body,
            statusCode: response.statusCode())
    }

    private JavaHttpRequest.Builder createRequestBuilder(){
        final builder =  JavaHttpRequest.newBuilder()
            .uri(URI.create(url))

        headers.each {k, v -> builder.setHeader(k, "$v") }

        if(!headers.containsValue("Accept")) {
            String acceptHeader = ""
            if (contentType !instanceof ContentType) {
                acceptHeader = contentType.acceptHeader
            } else {
                acceptHeader = "$contentType"
            }
            builder.setHeader("Accept", acceptHeader)
        }

        if(!headers.containsValue("Content-Type")) {
            String contentTypeHeader = ""
            if (requestContentType !instanceof ContentType) {
                contentTypeHeader = requestContentType.contentTypeStrings.first()
            } else {
                contentTypeHeader = "$requestContentType"
            }
            builder.setHeader("Content-Type", contentTypeHeader)
        }

        if(this.auth?.hasAuth()){
            def authMethod = this.auth.method
            switch(authMethod) {
                case HttpAuthBasic:
                    builder.setHeader("Authorization", "Basic $authMethod.encoded")
                    break
                case HttpAuthBearerToken:
                    builder.setHeader("Authorization", "Bearer $authMethod.token")
                    break
            }
        }

        switch(toMethod()){
            case Method.GET:
                builder.GET()
                break
            case Method.POST:
                builder.POST(HttpRequest.BodyPublishers.ofString(payload))
                break
            case Method.PUT:
                builder.PUT(HttpRequest.BodyPublishers.ofString(JsonOutput.toJson(payload)))
                break
            case Method.DELETE:
                builder.DELETE()
                break
        }
        builder
    }

    private String getPayload() {
        def payload = body

        if(requestContentType == ContentType.JSON && !body instanceof String)
            payload = JsonOutput.toJson(body)
        else if(requestContentType == ContentType.URLENC && body instanceof Map)
            payload = body.collect {k, v -> "$k=$v"}.join("&")

        payload
    }

    private void debugRequest(){
        if (this.debug) {
            log.info "${method.name()}: ${this.url}"
            if (this.body) {
                log.info "HTTP BODY: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.body))}"
                log.info "HTTP HEADERS: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.headers))}"
            }
        }
    }

    private Method toMethod() {
         method as Method ?: Method.valueOf("${method}".toString().toUpperCase())
    }

    private def handleSuccess(java.net.http.HttpResponse<String> resp){
        fillResponse(resp)

        if (this.debug) {
            log.info "HTTP SUCCESS: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.response.debugInfo))}"
        }

        success?.call(this.response)
    }

    private def handleError(java.net.http.HttpResponse<String> resp){

        fillResponse(resp)

        if (this.debug) {
            log.info "HTTP FAILURE: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.response.debugInfo))}"
        }

        final recovered = recover?.call(this.response)

        if (!recovered) {

            if (this.failWith) {
                final ex = this.failWith.call(this.response)
                if (ex instanceof Throwable) throw ex
            }

            error?.call(this.response)

            if (throwEx || throwIfNotOk)
                throw new Exception("Request failed with status ${resp.statusCode()}")
        }
    }
}
