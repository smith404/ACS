package com.k2.acs.model;

import java.time.LocalDate;

public interface DateCriteriaSummable {
    double getSumByDateCriteria(LocalDate date, String incurredDateComparison, String exposureDateComparison);
}
