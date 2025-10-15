package io.gtools.http

import java.nio.charset.StandardCharsets

class UriEncoder {

    static String encodeText(String text){
        URLEncoder.encode(text, StandardCharsets.UTF_8)
            .replace("+", "%20")
    }
}
