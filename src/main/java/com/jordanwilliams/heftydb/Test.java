package com.jordanwilliams.heftydb;

import com.jordanwilliams.heftydb.db.Config;
import com.jordanwilliams.heftydb.db.DB;
import com.jordanwilliams.heftydb.db.HeftyDB;
import com.jordanwilliams.heftydb.db.Record;
import com.jordanwilliams.heftydb.db.Snapshot;
import com.jordanwilliams.heftydb.util.CloseableIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.concurrent.Future;

public class Test {

    public static void main(String[] args) throws Exception {
        try {

            String directory = "tmp";

            //Open a HeftyDB in a directory
            DB testDB = HeftyDB.open(new Config.Builder().directory(Paths.get(directory)).build());

            String key = "hello";
            String value = "world";
            ByteBuffer someByteBufferKey = ByteBuffer.wrap(key.getBytes());
            ByteBuffer someByteBufferValue = ByteBuffer.wrap(value.getBytes());

            //Write a key
            Snapshot snapshot = testDB.put(someByteBufferKey, someByteBufferValue);

            //Read a key at a particular snapshot
            Record record = testDB.get(someByteBufferKey, snapshot);

            //Delete a key
            Snapshot deleteSnapshot = testDB.delete(someByteBufferKey);

            //Get an ascending iterator of keys greater than or equal
            //to the provided key at the provided snapshot
            CloseableIterator<Record> ascendingIterator = testDB.ascendingIterator(someByteBufferKey, snapshot);

            while (ascendingIterator.hasNext()) {
                Record next = ascendingIterator.next();
            }

            //Get a descending iterator of keys less than or equal
            //to the provided key at the provided snapshot
            CloseableIterator<Record> descendingIterator = testDB.descendingIterator(someByteBufferKey, snapshot);

            while (descendingIterator.hasNext()) {
                Record next = descendingIterator.next();
            }

            //Compact the database
            Future<?> compactionFuture = testDB.compact();
            compactionFuture.get();

            //Close the database
            testDB.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
