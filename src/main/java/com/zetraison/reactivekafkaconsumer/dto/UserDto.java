package com.zetraison.reactivekafkaconsumer.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserDto {
    @JsonProperty("registertime")
    private Long registerTime;

    @JsonProperty("userid")
    private String userId;

    @JsonProperty("regionid")
    private String regionId;

    @JsonProperty
    private String gender;
}
