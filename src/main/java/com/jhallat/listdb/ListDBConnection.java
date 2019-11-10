package com.jhallat.listdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ListDBConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ListDBConnection.class);

    private final int port;
    private final String host;
    private PrintWriter out;
    private BufferedReader in;
    private Socket socket;

    protected ListDBConnection(String host, int port) {
        this.port = port;
        this.host = host;
    }

    public void open() throws ListDBException {

        try {
            socket = new Socket(host, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            throw new ListDBException("Error connecting to ListDB Server", e);
        }

    }

    public void close() {
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

    private List<String> parseData(final String data) throws ListDBException {
        List<String> parsed = new ArrayList<>();
        char[] chars = data.toCharArray();
        if (chars.length == 0 || chars[0] != 'd') {
            throw new ListDBException("Invalid format for data response. Expected data header.");
        }
        if (chars.length < 2 || chars[1] != 'c') {
            throw new ListDBException("Invalid format for data response. Expected item count.");
        }
        int index = 2;
        StringBuilder parse_count = new StringBuilder();
        while (index < chars.length && chars[index] != ':') {
            if (Character.isDigit(chars[index])) {
                parse_count.append(chars[index]);
            } else {
                throw new ListDBException("Invalid format for data response. Invalid character in count.");
            }
            index++;
        }
        int count = Integer.parseInt(parse_count.toString());
        List<Integer> data_breaks = new ArrayList<>();
        index++;
        StringBuilder break_position = new StringBuilder();
        while (index < chars.length && data_breaks.size() < count) {
            if (Character.isDigit(chars[index])) {
                break_position.append(chars[index]);
            } else if (chars[index] == ':') {
                int break_pos = Integer.parseInt(break_position.toString());
                data_breaks.add(break_pos);
                break_position.delete(0, break_position.length());
            }
            index++;
        }
        for (Integer pos : data_breaks) {
            String item = new String(Arrays.copyOfRange(chars, index, index + pos));
            parsed.add(item);
            index += pos;
        }
        return parsed;
    }

    public List<String> getTopics() throws ListDBException {
        List<String> topics = new ArrayList<>();
        out.println("list topic");
        try {
            String response = in.readLine();
            topics = parseData(response);
        } catch (IOException e) {
            throw new ListDBException("Error getting topics", e);
        }
        return topics;
    }
}
