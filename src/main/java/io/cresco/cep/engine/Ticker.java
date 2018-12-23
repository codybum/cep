package io.cresco.cep.engine;

public class Ticker {
    String source;
    String urn;
    String metric;
    long ts;
    double value;

    public Ticker(String source, String urn, String metric, long ts, double value) {
        this.source = source;
        this.urn = urn;
        this.metric = metric;
        this.ts = ts;
        this.value = value;
    }
    public String getSource() {return source;}
    public String getUrn() {return urn;}
    public String getMetric() {return metric;}
    public long getTs() {return ts;}
    public double getValue() {return value;}

    @Override
    public String toString() {
        return "source:" + source + " urn:" + urn + " metric:" + metric + " timestamp:" + ts + " value:" + value;
    }
}
