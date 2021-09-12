package model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private List<Item> items;
    private PaymentState paymentStat;
    private  Double amount;
    private Customer customer;
}
