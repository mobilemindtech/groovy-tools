package io.gtools.http

import groovy.util.logging.Slf4j
import spock.lang.Specification

import static io.gtools.http.HttpClient.doGet
import static io.gtools.http.HttpClient.doGetAsync

//test-app unit:
@Slf4j
class HttpClientSpec extends Specification{


    void "simple http call"() {
        setup:
        given:
            def result = doGet {
                url = "http://tools.mobilemind.com.br/tools-api/mock/user"
            }
        expect:
        result.ok
        (result.value.body.name instanceof  String)
    }

    void "simple http call async"() {
        setup:
        given:
        def result = doGetAsync {
            url = "http://tools.mobilemind.com.br/tools-api/mock/user"
        }.get()
        expect:
        result.body.name instanceof  String
    }
}
