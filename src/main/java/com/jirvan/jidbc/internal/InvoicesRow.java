package com.jirvan.jidbc.internal;

import com.jirvan.dates.*;

import java.math.*;

public class InvoicesRow {

    public Long id;
    public Long merchantAbnNumber;
    public String invoiceNumber;
    public String status;
    public Integer daysOverdue;
    public Long termPaymentAgreementId;
    public Day invoiceDate;
    public Day dueDate;
    public Long debtorId;
    public BigDecimal outstandingAmount;

}
