package io.gtools.http


enum ContentType {
    ANY(new String[]{"*/*"}),
    TEXT(new String[]{"text/plain"}),
    JSON(new String[]{"application/json", "application/javascript", "text/javascript"}),
    XML(new String[]{"application/xml", "text/xml", "application/xhtml+xml", "application/atom+xml"}),
    HTML(new String[]{"text/html"}),
    URLENC(new String[]{"application/x-www-form-urlencoded"}),
    BINARY(new String[]{"application/octet-stream"}),
    PDF(new String[]{"application/pdf"});

    private final String[] ctStrings
    String[] getContentTypeStrings() { return ctStrings }
    @Override String toString() { return ctStrings[0] }

    String getAcceptHeader() {
        ctStrings.join(", ")
    }

    ContentType( String... contentTypes ) {
        this.ctStrings = contentTypes
    }
}
