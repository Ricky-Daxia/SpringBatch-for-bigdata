package com.example.dataprocessor.service;

import com.example.dataprocessor.model.Product;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

public class ProductWriter implements ItemWriter<Product> {

//    private static final Logger logger = LoggerFactory.getLogger(ProductWriter.class);
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ProductWriter(DataSource dataSource) {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    @Override
    public void write(@NonNull Chunk<? extends Product> chunk) {
        chunk.forEach(item -> {
            String sql1 = "insert into product (main_category, title, average_rating, features, description, price, store, details, parent_asin) values (:main_category, :title, :average_rating, :features, :description, :price, :store, :details, :parent_asin)";
            MapSqlParameterSource parameterSource1 = new MapSqlParameterSource();
            parameterSource1.addValue("main_category", item.getMain_category());
            parameterSource1.addValue("title", item.getTitle());
            parameterSource1.addValue("average_rating", item.getAverage_rating());
            List<String> features = item.getFeatures();
            if (!features.isEmpty()) {
                parameterSource1.addValue("features", item.getFeatures().get(0));
            } else {
                parameterSource1.addValue("features", null);
            }
            List<String> description = item.getDescription();
            if (!description.isEmpty()) {
                parameterSource1.addValue("description", description.get(0));
            } else {
                parameterSource1.addValue("description", null);
            }
            parameterSource1.addValue("price", item.getPrice());
            parameterSource1.addValue("store", item.getStore());

            try {
                parameterSource1.addValue("details", objectMapper.writeValueAsString(item.getDetails()));
            } catch (JsonProcessingException e) {
                parameterSource1.addValue("details", null);
            }

            parameterSource1.addValue("parent_asin", item.getParent_asin());
            KeyHolder keyHolder = new GeneratedKeyHolder();
            namedParameterJdbcTemplate.update(sql1, parameterSource1, keyHolder);
//            logger.info("thread id: " + Thread.currentThread().getId() + " " + item);
            List<Product.ImageInfo> images = item.getImages();
            if (!images.isEmpty()) {
                Long id = Objects.requireNonNull(keyHolder.getKeyAs(BigInteger.class)).longValue();
                Product.ImageInfo info = images.get(0);
                String url = info.getLarge();
                String sql2 = "insert into product_image (product_id, url, info) values (:id, :url, :info)";
                MapSqlParameterSource parameterSource2 = new MapSqlParameterSource();
                parameterSource2.addValue("id", id);
                parameterSource2.addValue("url", url);
                parameterSource2.addValue("info", info.getVariant());
                namedParameterJdbcTemplate.update(sql2, parameterSource2);
            }
        });
    }
}
