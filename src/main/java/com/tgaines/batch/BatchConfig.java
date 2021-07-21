package com.tgaines.batch;

import com.tgaines.batch.domain.StockPrice;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.net.MalformedURLException;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Value("schema-create.sql")
    private Resource createTableResource;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Bean
    public Job readJobFactory() {
        return jobBuilderFactory
                .get("readFile")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }

    @Bean
    public ItemProcessor<StockPrice, StockPrice> processor() {
        return new CustomProcessor();
    }

    public ItemWriter<StockPrice> jdbcWriter() {
        JdbcBatchItemWriter writer = new JdbcBatchItemWriterBuilder<StockPrice>()
                .sql("INSERT INTO STOCKPRICE (symbol, date, price) VALUES (:symbol, :date, :price)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<StockPrice>())
                .dataSource(dataSource)
                .build();

        writer.afterPropertiesSet();

        return writer;
    }

    @Bean
    public DataSourceInitializer dataSourceInitializer(DataSource dataSource)
            throws MalformedURLException {
        ResourceDatabasePopulator databasePopulator =
                new ResourceDatabasePopulator();

        databasePopulator.addScript(createTableResource);

        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        initializer.setDatabasePopulator(databasePopulator);

        return initializer;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1").<StockPrice, StockPrice>chunk(5)
                .reader(itemReader())
                .writer(jdbcWriter())
                .processor(processor())
                .listener(new ItemCountListener())
                .build();
    }

    @Bean
    public ItemReader<StockPrice> itemReader() {
        return new FlatFileItemReaderBuilder<StockPrice>()
                .name("stockReader")
                .resource(new ClassPathResource("stocks.txt"))
                .lineMapper(stockMapper())
                .build();
    }

    private LineMapper<StockPrice> stockMapper() {
        DefaultLineMapper<StockPrice> studentLineMapper = new DefaultLineMapper<>();

        LineTokenizer lineTokenizer = tokenizer();
        studentLineMapper.setLineTokenizer(lineTokenizer);

        FieldSetMapper<StockPrice> studentInformationMapper =
                mapper();
        studentLineMapper.setFieldSetMapper(studentInformationMapper);

        return studentLineMapper;
    }

    private LineTokenizer tokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setNames(new String[]{
                "symbol",
                "price",
                "date"
        });
        return tokenizer;
    }

    private FieldSetMapper<StockPrice> mapper() {
        BeanWrapperFieldSetMapper<StockPrice> studentInformationMapper =
                new BeanWrapperFieldSetMapper<>();
        studentInformationMapper.setTargetType(StockPrice.class);
        return studentInformationMapper;
    }

    public static class ItemCountListener implements ChunkListener {

        @Override
        public void beforeChunk(ChunkContext context) {
        }

        @Override
        public void afterChunk(ChunkContext context) {
            int count = context.getStepContext().getStepExecution().getReadCount();
            System.out.println("ItemCount: " + count);
        }

        @Override
        public void afterChunkError(ChunkContext context) {
        }
    }

}
