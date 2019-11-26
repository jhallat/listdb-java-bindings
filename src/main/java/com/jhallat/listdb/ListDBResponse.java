package com.jhallat.listdb;

public class ListDBResponse {

    private final ListDBResponseType responseType;
    private final String contents;

    public ListDBResponse(ListDBResponseType responseType, String contents) {
        this.responseType = responseType;
        this.contents = contents;
    }

    public ListDBResponseType getResponseType() {
        return responseType;
    }

    public String getContents() {
        return contents;
    }

    @Override
    public String toString() {
        String value = String.format("[responseType:%s, contents:%s]", responseType, contents);
        return value;
    }

}
