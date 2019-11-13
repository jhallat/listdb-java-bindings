package com.jhallat.listdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

class ListDBSocket {

    private static final Logger LOG = LoggerFactory.getLogger(ListDBConnection.class);

    private final String hostname;
    private final int port;
    private PrintWriter out;
    private BufferedReader in;
    private Socket socket;

    protected ListDBSocket(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    protected void open() throws ListDBException {

        try {
            socket = new Socket(hostname, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            throw new ListDBException("Error connecting to ListDB Server", e);
        }

    }

    protected void close() {
        if (out != null) {
            out.close();
        }
        if (in != null) {
            try {
                in.close();
            } catch (IOException e) {
                //Do nothing. Not necessary to catch an exception on close
                LOG.error("Error closing socket input stream", e);
            }
        }
        try {
            socket.close();
        } catch (IOException e) {
            //Do nothing. Not necessary to catch an exception on close
            LOG.error("Error closing socket input stream", e);
        }
    }

    protected String request(String request) throws ListDBException {

        out.println(request);
        try {
            String response = in.readLine();
            return response;
        } catch (IOException e) {
            throw new ListDBException("Error getting request", e);
        }

    }
}
