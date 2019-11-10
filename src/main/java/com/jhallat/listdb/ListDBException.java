package com.jhallat.listdb;

public class ListDBException extends Exception {

    public ListDBException(String message, Exception exception) {
        super(message, exception);
    }

    public ListDBException(String message) {
        super(message);
    }

}
