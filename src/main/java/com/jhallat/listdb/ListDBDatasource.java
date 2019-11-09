package com.jhallat.listdb;

public class ListDBDatasource {

    private final int port;

    public ListDBDatasource(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public ListDBConnection connect() {
        ListDBConnection connection = new ListDBConnection(port);
        connection.open();
        return connection;
    }
}
