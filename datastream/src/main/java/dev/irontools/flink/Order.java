package dev.irontools.flink;

import java.util.Objects;

public class Order {
    private String orderId;
    private String customerName;
    private String category;
    private Double amount;
    private String productName;
    private Long timestamp;

    public Order() {
    }

    public Order(String orderId, String customerName, String category, Double amount, String productName, Long timestamp) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.category = category;
        this.amount = amount;
        this.productName = productName;
        this.timestamp = timestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Objects.equals(orderId, order.orderId) &&
                Objects.equals(customerName, order.customerName) &&
                Objects.equals(category, order.category) &&
                Objects.equals(amount, order.amount) &&
                Objects.equals(productName, order.productName) &&
                Objects.equals(timestamp, order.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, customerName, category, amount, productName, timestamp);
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", customerName='" + customerName + '\'' +
                ", category='" + category + '\'' +
                ", amount=" + amount +
                ", productName='" + productName + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
