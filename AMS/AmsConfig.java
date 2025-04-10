import lombok.Data;
import java.util.List;
import java.time.LocalDate;
import java.time.ZoneId;

@Data
public class AmsConfig {
    private Date lob;
    private boolean showPastFuture;
    private String cashFlowFrequency;
    private Date cashFlowStart;
    private Date cashFlowEnd;
    private double amount;
    private String currency;
    private String toa;
    private int duration;
    private boolean calendar;
    private Date contractDate;
    private String patternType;
    private String factor;
    private List<Element> elements;

    public LocalDate getLobAsLocalDate() {
        return lob != null ? lob.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getCashFlowStartAsLocalDate() {
        return cashFlowStart != null ? cashFlowStart.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getCashFlowEndAsLocalDate() {
        return cashFlowEnd != null ? cashFlowEnd.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    public LocalDate getContractDateAsLocalDate() {
        return contractDate != null ? contractDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null;
    }

    @Data
    public static class Element {
        private String type;
        private Double initial;
        private Double distribution;
    }
}
