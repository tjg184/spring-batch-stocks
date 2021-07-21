package com.tgaines.batch;

import com.tgaines.batch.domain.StockPrice;
import org.springframework.batch.item.ItemProcessor;

public class CustomProcessor implements ItemProcessor<StockPrice, StockPrice> {
    @Override
    public StockPrice process(StockPrice item) throws Exception {
        System.out.println("Processing " + item.getSymbol() + ", " + item.getPrice());
        return item;
    }
}
