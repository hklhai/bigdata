package cn.edu.ncut.bigdata.advance.streaming.kafka;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;

/**
 * Created by Ocean lin on 2017/12/18.
 */
public class JavaCustomReceiver extends Receiver {
    String host = null;
    int port = -1;

    public JavaCustomReceiver(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread() {
            @Override
            public void run() {
                receive();
            }
        }.start();
    }

    @Override
    public void onStop() {

    }

    /**
     * Create a socket connection and receive data until receiver is stopped
     */
    private void receive() {
        Socket socket = null;
        String userInput = null;

        try {
            // connect to the server
            socket = new Socket(host, port);

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Until stopped or connection broken continue reading
            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");
                store(userInput);
            }
            reader.close();
            socket.close();

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch (ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch (Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }
}
