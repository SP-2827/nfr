package com.expirement.nfr;

import lombok.extern.log4j.Log4j2;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Log4j2
public class WriteObjectWorkerThread implements Runnable {

    private final CountDownLatch latch;
    private final List<PurchaseOrder> purchaseOrders;

    public WriteObjectWorkerThread(final CountDownLatch latch, final List<PurchaseOrder> purchaseOrders) {
        this.latch = latch;
        this.purchaseOrders = purchaseOrders;
    }

    @Override
    public void run() {
        purchaseOrders.forEach(purchaseOrder -> {
            try (final ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(String.format("D:\\test\\MyObject_%s.ser", purchaseOrder.getId())))) {
                out.writeObject(purchaseOrder);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        latch.countDown();
    }
}
