package com.jhallat.listdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Topic {

    private static final Logger LOG = LoggerFactory.getLogger(Topic.class);

    private final ListDBSocket socket;
    private final String id;
    private final ListDBParser parser = new ListDBParser();

    protected Topic(String id, ListDBSocket socket) {
        this.socket = socket;
        this.id = id;
    }

    protected void open() throws ListDBException {
        socket.open();
        ListDBResponse response = socket.request("open topic " + id);
        LOG.debug(response.getContents());
    }

    public List<Record> list() throws ListDBException {
        ListDBResponse response = socket.request("list");
        LOG.debug("{}", response);
        if (response.getResponseType() == ListDBResponseType.DATA) {
            return parser.parseData(response.getContents());
        } else {
            parser.handleError(response);
        }
        throw new ListDBException("UNKNOWN");
    }
}
