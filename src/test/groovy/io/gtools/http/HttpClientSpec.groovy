package io.gtools.http

import groovy.util.logging.Slf4j
import org.junit.Assert
import org.junit.Test

import static io.gtools.http.HttpClient.http

//test-app unit:
@Slf4j
class HttpClientSpec {


    @Test
    void testHttp() {

        def client =
            http {
                url = "http://tools.mobilemind.com.br/tools-api/mock/user"
            }

        client.get()
            .ifFailure {
                log.info "err"
            }
            .ifOk {
                log.info("succ = $it")
            }
            .foreach {
                log.info("JSON=${it}, body = ${it.body}")
            }

        log.info "end"


        Assert.assertTrue(true)
    }
}
