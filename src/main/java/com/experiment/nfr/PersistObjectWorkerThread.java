package com.experiment.nfr;

import lombok.extern.log4j.Log4j2;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Log4j2
public class PersistObjectWorkerThread implements Runnable {

    private final CountDownLatch latch;
    private final List<PurchaseOrder> purchaseOrders;
    private final DataSource dataSource;

    public PersistObjectWorkerThread(final CountDownLatch latch, final List<PurchaseOrder> purchaseOrders, final DataSource dataSource) {
        this.latch = latch;
        this.purchaseOrders = purchaseOrders;
        this.dataSource = dataSource;
    }

    @Override
    public void run() {
        try (final PreparedStatement preparedStatement = dataSource.getConnection().prepareStatement("insert into purchase_order(id,product_name,price,quantity) values(?,?,?,?)")) {
            purchaseOrders.forEach(purchaseOrder -> {
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
            });
            // Step 3: Execute the query or update query
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            log.error(e);
            throw new RuntimeException("Failed to Insert");
        }
        latch.countDown();
    }
}
