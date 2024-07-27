package com.example.dataprocessor.service;

import com.example.dataprocessor.model.Product;
import lombok.NonNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

public class ProductUrlChecker implements ItemWriter<Product> {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public ProductUrlChecker(DataSource dataSource) {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    private BigInteger fibonacci(BigInteger n) {
        if (n.compareTo(BigInteger.ONE) < 0 || n.compareTo(BigInteger.ZERO) == 0) {
            return n;
        }
        return fibonacci(n.subtract(BigInteger.ONE)).add(fibonacci(n.subtract(BigInteger.ONE).subtract(BigInteger.ONE)));
    }

    @Override
    public void write(@NonNull Chunk<? extends Product> chunk) {
//        System.out.println(fibonacci(new BigInteger(String.valueOf(20))));
        chunk.forEach(item -> {
            List<Product.ImageInfo> images = item.getImages();
            if (images.isEmpty()) {
                return;
            }
            Product.ImageInfo info = images.get(0);
            String url = info.getLarge();
            boolean available = true;
            try {
                Document document = Jsoup.connect(url).get();
            } catch (IOException e) {
                System.err.println("url " + url + " not available");
                available = false;
            }
            if (!available) {
                String sql = "insert into bad_image (url) values (:url)";
                MapSqlParameterSource parameterSource = new MapSqlParameterSource();
                parameterSource.addValue("url", url);
                namedParameterJdbcTemplate.update(sql, parameterSource);
            }
        });
    }

}
