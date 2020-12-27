package com.experiment.nfr;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@Data
@Builder
@AllArgsConstructor
public class PurchaseOrder implements Serializable {

    private Integer id;
    private String productName;
    private Double price;
    private Integer quantity;

}
