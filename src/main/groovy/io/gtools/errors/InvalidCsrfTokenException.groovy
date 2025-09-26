package io.gtools.errors

class InvalidCsrfTokenException extends RuntimeException {
    InvalidCsrfTokenException() {
        super("Invalid CSRF token")
    }

}
