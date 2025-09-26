package io.gtools.http

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import groovy.json.JsonOutput


class HttpResponse {
    Object rawResp
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

    def <T> T decode(Class<T> cls, GsonBuilder builder = null) {
        builder = builder ?: new Gson().newBuilder()
        builder.create().fromJson(body, cls)
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
}