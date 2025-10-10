package io.gtools.http

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import groovy.json.JsonOutput


class HttpResponse {
    Object rawResp
    Object rawBody
    Object body
    int statusCode
    Object statusLine

    Map getBodyAsMap() {
        body as Map
    }

    List getBodyAsList() {
        body as List
    }

    String getBodyAsString() {
        "$body"
    }

    String getBodyAsJsonStr() {
        JsonOutput.toJson(body)
    }

    boolean isOk() { statusCode == 200 }

    def <T> T decode(Class<T> cls, GsonBuilder builder = null) {
        builder = builder ?: new Gson().newBuilder()
        builder.create().fromJson(body ?: rawBody, cls)
    }

    def propertyMissing(String propertyName) {
        rawResp."$propertyName"
    }

    def getDebugInfo() {
        [
                body       : this.body,
                status_text: this.statusLine,
                status_code: this.statusCode,
        ]
    }

    String toString() {
        JsonOutput.prettyPrint(
            JsonOutput.toJson([
                body       : this.body,
                status_text: this.statusLine,
                status_code: this.statusCode,
            ]))
    }
}