package com.k2.acs.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = false)
public class BestEstimateCashFlow extends PropertyObject {
    private List<CashFlow> cashFlows;
    private boolean filterZero = true;

    public BestEstimateCashFlow() {
        cashFlows = new ArrayList<>();
    }

    public double getTotalAmount() {
        return cashFlows.stream()
                .mapToDouble(CashFlow::getAmount)
                .sum();
    }

    public int loadCashFlows(List<CashFlow> cashFlows) {
        this.cashFlows.clear();
        if (filterZero) {
            this.cashFlows.addAll(cashFlows.stream()
                    .filter(cashFlow -> cashFlow.getAmount() != 0)
                    .toList());
        } else {
            this.cashFlows.addAll(cashFlows);
        }
        return this.cashFlows.size();
    }

    public boolean addCashFlow(CashFlow cashFlow) {
        if (filterZero && cashFlow.getAmount() == 0) {
            return false;
        }
        this.cashFlows.add(cashFlow);
        return true;
    }

    public boolean removeCashFlow(CashFlow cashFlow) {
        return this.cashFlows.remove(cashFlow);
    }

    public void sortCashFlows(boolean ascending) {
        cashFlows.sort((cf1, cf2) -> {
            int result = cf1.getIncurredDate().compareTo(cf2.getIncurredDate());
            if (result == 0) {
                result = cf1.getSettlementDate().compareTo(cf2.getSettlementDate());
            }
            if (result == 0) {
                result = cf1.getDueDate().compareTo(cf2.getDueDate());
            }
            if (result == 0) {
                result = cf1.getReportedDate().compareTo(cf2.getReportedDate());
            }
            return ascending ? result : -result;
        });
    }

    // Overload for default ascending sort
    public void sortCashFlows() {
        sortCashFlows(true);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString()).append("Flows:");
        sb.append(String.format("%n%-15s %-15s %-15s %-15s %-15s %-15s%n", "Amount", "Currency", "IncurredDate", "ReportedDate", "DueDate", "SettlementDate"));
        sb.append("------------------------------------------------------------------------------------------\n");
        for (CashFlow cashFlow : cashFlows) {
            sb.append(String.format("%15.5f %-15s %-15s %-15s %-15s %-15s%n",
                    cashFlow.getAmount(),
                    cashFlow.getCurrency(),
                    cashFlow.getIncurredDate(),
                    cashFlow.getReportedDate(),
                    cashFlow.getDueDate(),
                    cashFlow.getSettlementDate()));
        }
        return sb.toString();
    }
}
