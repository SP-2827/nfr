package com.experiment.nfr;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.util.StopWatch;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Log4j2
public class NonFunctionalResourceExperiment {

    public static final int ORDER_LIMIT = 10000;
    public static final int THREADS = 3;
    private final BasicDataSource dataSource = getConnection();

    public void runExperiment() {
        final List<PurchaseOrder> purchaseOrders = new ArrayList<>();
        log.info("started adding purchase ");
        for (int i = 0; i < ORDER_LIMIT; i++) {
            purchaseOrders.add(PurchaseOrder.builder().id(i + 1).price(1000d).productName(UUID.randomUUID().toString()).quantity(i + 5).build());
        }
        log.info("completed adding purchase ");
        trySingleThreaded(purchaseOrders);
        tryMultiThreaded(purchaseOrders);
    }

    private void trySingleThreaded(final List<PurchaseOrder> purchaseOrders) {
        log.info("-----------------------------------------------------------------------------------");
        log.info("File System - One Thread: {} Objects", ORDER_LIMIT);
        tryFileSystemInOneThread(purchaseOrders);
        log.info("-----------------------------------------------------------------------------------");
        log.info("IN-Memory - One Thread: {} Objects", ORDER_LIMIT);
        tryH2DBInOneThread(purchaseOrders);
    }

    private void tryMultiThreaded(final List<PurchaseOrder> purchaseOrders) {
        final int chunkSize = 250;
        final AtomicInteger counter = new AtomicInteger();

        final Collection<List<PurchaseOrder>> collection = purchaseOrders.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                .values();
        log.info("-----------------------------------------------------------------------------------");
        log.info("File System - Multi Thread: {} Objects", ORDER_LIMIT);
        tryFileSystemInMultiThread(collection);
        log.info("-----------------------------------------------------------------------------------");
        log.info("In-Memory - Multi Thread: {} Objects", ORDER_LIMIT);
        tryH2DBInMultiThread(collection);
        log.info("-----------------------------------------------------------------------------------");
    }

    private void tryH2DBInMultiThread(final Collection<List<PurchaseOrder>> collection) {
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start("Write Objects into InMemory - Multi Thread");
        final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        final CountDownLatch latch = new CountDownLatch(THREADS);
        try (final Connection connection = dataSource.getConnection()) {
            // Step 2:Create a statement using connection object
            final Statement statement = connection.createStatement();
            statement.execute("drop table if exists purchase_order;");
            // Step 3: Execute the query or update query
            statement.execute("create table purchase_order(id int(8) primary key,product_name varchar(180),price float(8),quantity int(8));");
        } catch (SQLException e) {
            log.error(e);
        }
        collection.forEach(purchaseOrderSubList -> executor.submit(new PersistObjectWorkerThread(latch, purchaseOrderSubList, dataSource)));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error(e);
            throw new RuntimeException("Failed CountDownLatch");
        }
        stopWatch.stop();
        printTat(stopWatch);
    }

    private void tryFileSystemInMultiThread(final Collection<List<PurchaseOrder>> purchaseOrders) {
        final StopWatch fileWriteStopWatch = new StopWatch();
        fileWriteStopWatch.start("Write Objects into File System - Multi Thread");
        final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        final CountDownLatch latch = new CountDownLatch(THREADS);
        purchaseOrders.forEach(purchaseOrderSubList -> executor.submit(new WriteObjectWorkerThread(latch, purchaseOrderSubList)));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error(e);
            throw new RuntimeException("Failed CountDownLatch");
        }
        fileWriteStopWatch.stop();
        printTat(fileWriteStopWatch);
    }

    private void tryFileSystemInOneThread(final List<PurchaseOrder> purchaseOrders) {
        final StopWatch fileWriteStopWatch = new StopWatch();
        fileWriteStopWatch.start("Write Objects into File System - Single Thread");
        purchaseOrders.forEach(this::writeObject);
        fileWriteStopWatch.stop();
        printTat(fileWriteStopWatch);
    }

    private void writeObject(final PurchaseOrder purchaseOrder) {
        try (final ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(String.format("D:\\test\\MyObject_%s.ser", purchaseOrder.getId())))) {
            out.writeObject(purchaseOrder);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException("Failed to Write into File System");
        }
    }

    private void tryH2DBInOneThread(final List<PurchaseOrder> purchaseOrders) {
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start("Write Objects into InMemory - Single Thread");
        try (final Connection connection = dataSource.getConnection();
             // Step 2:Create a statement using connection object
             final Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists purchase_order;");
            // Step 3: Execute the query or update query
            statement.execute("create table purchase_order(id int(8) primary key,product_name varchar(180),price float(8),quantity int(8));");
        } catch (SQLException e) {
            log.error(e);
        }
        try (final Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (final PreparedStatement preparedStatement = connection.prepareStatement("insert into purchase_order(id,product_name,price,quantity) values(?,?,?,?)")) {
                purchaseOrders.forEach(purchaseOrder -> persistObject(preparedStatement, purchaseOrder));
                // Step 3: Execute the query or update query
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                log.error(e);
                throw new RuntimeException("Failed to Insert");
            }
            connection.commit();
        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("Failed to Insert");
        }
        stopWatch.stop();
        printTat(stopWatch);
    }

    private void persistObject(final PreparedStatement preparedStatement, final PurchaseOrder purchaseOrder) {
        try {
            preparedStatement.setInt(1, purchaseOrder.getId());
            preparedStatement.setString(2, purchaseOrder.getProductName());
            preparedStatement.setDouble(3, purchaseOrder.getPrice());
            preparedStatement.setInt(4, purchaseOrder.getQuantity());
            preparedStatement.addBatch();
        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("Failed to Insert");
        }
    }

    private void printTat(final StopWatch stopWatch) {
        log.info("Time Elapsed for {}: {} seconds", stopWatch.currentTaskName(), stopWatch.getTotalTimeSeconds());
        log.info(stopWatch.prettyPrint());
    }

    private BasicDataSource getConnection() {
        final BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUrl("jdbc:h2:~/test1");
        basicDataSource.setDriverClassName("org.h2.Driver");
        basicDataSource.setUsername("sa");
        basicDataSource.setPassword("");
        basicDataSource.setMaxTotal(10);
        return basicDataSource;
    }
}
