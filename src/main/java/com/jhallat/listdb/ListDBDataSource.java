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

    public Directory getRootDirectory() throws ListDBException {
        Directory connection = new Directory(host, port);
        connection.open();
        return connection;
    }

    public Directory getDirectoryFromPath(String path) throws ListDBException {
        Directory connection = new Directory(host, port, path);
        connection.open();
        return connection;
    }
}
