package io.gtools.http

import groovy.json.JsonOutput
import io.gio.Result
import groovy.util.logging.Slf4j
import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.Method
import static io.gio.Result.tryOf

@Slf4j
class HttpClient {

    Object method = Method.GET
    Object contentType = ContentType.JSON
    Object requestContentType = ContentType.JSON
    Boolean asJson = false
    @Deprecated
    boolean throwEx
    boolean throwIfNotOk
    Object body
    String url
    boolean debug
    Map headers = [:]
    Closure success
    Closure error
    Closure executor
    Closure recover
    Closure failWith

    HttpResponse response


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

    static Result<HttpResponse> doPost(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).post()
    }

    static Result<HttpResponse> doPut(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).put()
    }

    static Result<HttpResponse> doDelete(@DelegatesTo(HttpClient) Closure c) {
        new HttpClient(c).delete()
    }


    Result<HttpResponse> post() { request(Method.POST) }

    Result<HttpResponse> get() { request(Method.GET) }

    Result<HttpResponse> put() { request(Method.PUT) }

    Result<HttpResponse> delete() { request(Method.DELETE) }

    Result<HttpResponse> exec() { request() }

    Result<HttpResponse> request(Object method = null) {
        if (method) this.method = method
        tryOf {
            this.execute()
            this.response
        }
    }


    private void fillResponse(Object resp, Object reader) {
        this.response = new HttpResponse(
                rawResp: resp,
                body: reader,
                statusCode: resp.statusLine.statusCode,
                statusLine: resp.statusLine)
    }

    private void execute() {
        this.executor.call()
        def http = new HTTPBuilder(url)
        http.headers = headers

        /*
        http.handler.failure = { resp, reader ->
            fillResponse(resp, reader)

            def recovered = recover?.call(this.response)

            if (!recovered) {
                error?.call(this.response)

                if (throwEx)
                    throw new Exception("Request failed with status ${resp.status} - ${resp.statusLine}")
            }

        }*/


        def m = (method as Method) ?: Method.valueOf("${method}".toString().toUpperCase())

        if (this.debug) {
            log.info "${method.name()}: ${this.url}"
            if (this.body) {
                log.info "HTTP BODY: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.body))}"
                log.info "HTTP HEADERS: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.headers))}"
            }
        }

        http.request(m, contentType) { req ->
            delegate.requestContentType = this.requestContentType
            delegate.body = this.body
            delegate.response.success = { resp, reader ->

                fillResponse(resp, reader)

                if (this.debug) {
                    log.info "HTTP SUCCESS: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.response.debugInfo))}"
                }

                success?.call(this.response)
            }
            delegate.response.failure = { resp, reader ->
                fillResponse(resp, reader)

                if (this.debug) {
                    log.info "HTTP FAILURE: ${JsonOutput.prettyPrint(JsonOutput.toJson(this.response.debugInfo))}"
                }

                def recovered = recover?.call(this.response)

                if (!recovered) {

                    if (this.failWith) {
                        def ex = this.failWith.call(this.response)
                        if (ex instanceof Throwable) throw ex
                    }

                    error?.call(this.response)

                    if (throwEx || throwIfNotOk)
                        throw new Exception("Request failed with status ${resp.status} - ${resp.statusLine}")
                }
            }
        }
    }
}
