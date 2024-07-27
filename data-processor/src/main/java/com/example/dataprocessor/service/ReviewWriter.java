package com.example.dataprocessor.service;

import com.example.dataprocessor.model.Review;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

public class ReviewWriter implements ItemWriter<Review> {

//    private static final Logger logger = LoggerFactory.getLogger(ReviewWriter.class);

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public ReviewWriter(DataSource dataSource) {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    @Override
    public void write(@NonNull Chunk<? extends Review> chunk) {
//        LocalDateTime now = LocalDateTime.now();
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//        String formattedDateTime = now.format(formatter);
//        System.out.println(formattedDateTime + " writer thread id: " + Thread.currentThread().getId());
        chunk.forEach(item -> {
            String sql1 = "insert into review (rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote) values (:rating, :title, :text, :asin, :parent_asin, :user_id, :timestamp, :helpful_vote)";
            MapSqlParameterSource parameterSource1 = new MapSqlParameterSource();
            parameterSource1.addValue("rating", item.getRating());
            parameterSource1.addValue("title", item.getTitle());
            parameterSource1.addValue("text", item.getText());
            parameterSource1.addValue("asin", item.getAsin());
            parameterSource1.addValue("parent_asin", item.getParent_asin());
            parameterSource1.addValue("user_id", item.getUser_id());
            parameterSource1.addValue("timestamp", item.getTimestamp());
            parameterSource1.addValue("helpful_vote", item.getHelpful_vote());
            KeyHolder keyHolder = new GeneratedKeyHolder();
            namedParameterJdbcTemplate.update(sql1, parameterSource1, keyHolder);
//            logger.info("thread id: " + Thread.currentThread().getId() + " " + item);
            String sql2 = "insert into user (user_id, review_id) values (:user_id, :review_id)";
            MapSqlParameterSource parameterSource2 = new MapSqlParameterSource();
            parameterSource2.addValue("user_id", item.getUser_id());
            Long id = Objects.requireNonNull(keyHolder.getKeyAs(BigInteger.class)).longValue();
            parameterSource2.addValue("review_id", id);
            namedParameterJdbcTemplate.update(sql2, parameterSource2);
            List<Review.ImageInfo> images = item.getImages();
            if (!images.isEmpty()) {
                Review.ImageInfo info = images.get(0);
                String url = info.getMedium_image_url();
                String sql3 = "insert into review_image (review_id, url, info) values (:id, :url, :info)";
                MapSqlParameterSource parameterSource3 = new MapSqlParameterSource();
                parameterSource3.addValue("id", id);
                parameterSource3.addValue("url", url);
                parameterSource3.addValue("info", info.getAttachment_type());
                namedParameterJdbcTemplate.update(sql3, parameterSource3);
            }
        });
    }
}

