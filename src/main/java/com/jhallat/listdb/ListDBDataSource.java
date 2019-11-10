package com.jhallat.listdb;

public class ListDBDataSource {

    private final int port;
    private final String host;

    public ListDBDataSource(String host, int port) {
        this.port = port;
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public ListDBConnection connect() throws ListDBException {
        ListDBConnection connection = new ListDBConnection(host, port);
        connection.open();
        return connection;
    }
}
