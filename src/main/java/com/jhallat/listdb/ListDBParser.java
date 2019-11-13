package com.jhallat.listdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ListDBParser {

    private static final Logger LOG = LoggerFactory.getLogger(ListDBParser.class);

    protected List<Record> parseData(final String data) throws ListDBException {
        LOG.debug("data rcvd: {}", data);
        List<Record> parsed = new ArrayList<>();
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
            LOG.debug("item = '{}'", item);
            String key = item.substring(0,36);
            String value = item.substring(36);
            LOG.debug("[key,value] = ['{}','{}']", key, value);
            parsed.add(new Record(key, value));
            index += pos;
        }
        return parsed;
    }

}
