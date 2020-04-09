package com.gooyuanly.kafka.metrics.collector.kafka;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

/**
 * author: houying
 * date  : 17-2-8
 * desc  :
 */
public class ZkSerializerImpl implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        try {
            return ((String) data).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        try {
            return bytes == null ? null : new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
