/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package tampilan;


import java.awt.Color;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.swing.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

public class Konsumerbooking extends JFrame {
    private JTextArea feedbackArea;
    private KafkaConsumer<String, String> consumer;
    private Thread consumerThread;
    private Connection dbConnection;

    public Konsumerbooking() {
        setTitle("Kafka Konsumer Booking Admin");
        setSize(500, 400);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(null);
        
        getContentPane().setBackground(new Color(28, 118, 90));

        JLabel feedbackLabel = new JLabel("User Feedback:");
        feedbackLabel.setBounds(20, 20, 150, 25);
        feedbackLabel.setForeground(Color.WHITE);
        add(feedbackLabel);

        feedbackArea = new JTextArea();
        feedbackArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(feedbackArea);
        scrollPane.setBounds(20, 50, 450, 250);
        add(scrollPane);
        
        JButton closeButton = new JButton("Close");
        closeButton.setBounds(200, 320, 100, 30);
        closeButton.addActionListener(e -> {
            stopConsumer();// Hentikan Kafka Consumer dengan aman
            dispose(); // Tutup jendela
        });
        add(closeButton);

        // Setup the consumer when the application starts
        establishDatabaseConnection();
        startConsumer();

        setVisible(true);
    }
    
    private void stopConsumer() {
        if (consumerThread != null && consumerThread.isAlive()) {
            consumerThread.interrupt(); // Hentikan thread konsumer
        }
        if (consumer != null) {
            consumer.close(); // Tutup Kafka consumer
        }else{
            consumerThread.interrupt(); // Reset status interrupt
            

        }
        feedbackArea.append("Kafka Consumer stopped successfully.\n");
    }

    private void establishDatabaseConnection() {
        try {
            String url = "jdbc:mysql://localhost:3306/last"; // Ganti dengan nama database Anda
            String user = "root"; // Ganti dengan username MySQL Anda
            String password = ""; // Ganti dengan password MySQL Anda
            dbConnection = DriverManager.getConnection(url, user, password);
            feedbackArea.append("Database connected successfully.\n");
        } catch (SQLException e) {
            feedbackArea.append("Error connecting to database: " + e.getMessage() + "\n");
        }
    }

    private void startConsumer() {
        // Kafka Consumer Configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.29.167:9092, 192.168.29.35:9092, 192.168.29.45:9092"); // Kafka server address
        props.put("group.id", "bookingg-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // Start consuming from the earliest message

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("topik-bookinguser"));

        consumerThread = new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100); // Wait for messages
                    for (ConsumerRecord<String, String> record : records) {
                        String feedbackMessage = record.value();
                        feedbackArea.append("Received Feedback: " + feedbackMessage + "\n\n");
                        saveToDatabase(feedbackMessage);
                    }
                }
            } catch (Exception ex) {
                feedbackArea.append("Kafka Konsumer Dimatikan");
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        });

        consumerThread.start();
        feedbackArea.append("Kafka Consumer started successfully.\n");
    }
    
    private void saveToDatabase(String message) {
        try {
            // Mengasumsikan message dalam format sederhana: {"userid": 1, "nomormeja": 3, "nama": "abu", "paketid": 2, "waktu": "11 am", "harga": 20000, "status": "Pending"}
            message = message.replace("{", "").replace("}", "").replace("\"", "");
            String[] parts = message.split(",");

            int Userid = Integer.parseInt(parts[0].split(":")[1].trim());  // Trim spasi agar tidak ada kesalahan
            int nomorMeja = Integer.parseInt(parts[1].split(":")[1].trim());  // Trim spasi agar tidak ada kesalahan
            String nama = parts[2].split(":")[1].trim();
            String waktu = parts[4].split(":")[1].trim();  // Waktu diambil sebagai string
            int paketId = Integer.parseInt(parts[3].split(":")[1].trim());  // Trim spasi agar tidak ada kesalahan
            String harga = parts[5].split(":")[1].trim();

            // Query SQL Anda
            String query = "INSERT INTO booking (userid, mejaid, nama, paketid, harga, status, waktu) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = dbConnection.prepareStatement(query);
            stmt.setInt(1, Userid);
            stmt.setInt(2, nomorMeja);
            stmt.setString(3, nama);
            stmt.setInt(4, paketId);
            stmt.setString(5, harga);  // Set waktu sebagai String
            stmt.setString(6, "Pending");
            stmt.setString(7, waktu);

            stmt.executeUpdate();
            feedbackArea.append("Feedback berhasil disimpan ke database.\n");
        } catch (Exception e) {
            feedbackArea.append("Terjadi kesalahan saat menyimpan feedback ke database: " + e.getMessage() + "\n");
        }
    }
    
    public static void main(String[] args) {
        new Konsumerbooking(); // Start the application
    }
}