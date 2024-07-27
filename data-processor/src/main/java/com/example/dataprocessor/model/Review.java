package com.example.dataprocessor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Review implements Serializable {

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ImageInfo {

        private String medium_image_url;
        private String attachment_type;
    }

    @Serial
    private static final long serialVersionUID = 1L;

    private double rating;

    private String title;

    private String text;

    private List<ImageInfo> images;

    private String asin;

    private String parent_asin;

    private String user_id;

    private long timestamp;

    private int helpful_vote;
}
