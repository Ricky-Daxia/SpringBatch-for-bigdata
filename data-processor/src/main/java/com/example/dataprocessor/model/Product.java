package com.example.dataprocessor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Product implements Serializable {

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ImageInfo {

        private String large;
        private String variant;
    }

    @Serial
    private static final long serialVersionUID = 1L;

    private String main_category;

    private String title;

    private double average_rating;

    private List<String> features;

    private List<String> description;

    private double price;

    private List<ImageInfo> images;

    private String store;

    private List<String> categories;

    private Map<String, Object> details;

    private String parent_asin;
}
