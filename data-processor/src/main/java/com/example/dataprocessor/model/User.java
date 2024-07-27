package com.example.dataprocessor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class User implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String user_id;

    private String asin;
}
