package join.Demoz;

/**
 * Create By 鸣宇淳 on 2020/6/4
 **/
public class CityInfo {
    private Integer cityId;
    private String cityName;
    private Long ts;

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getCityId() {
        return cityId;
    }

    public String getCityName() {
        return cityName;
    }

    public Long getTs() {
        return ts;
    }

    public CityInfo() {
    }

    public CityInfo(Integer cityId, String cityName, Long ts) {
        this.cityId = cityId;
        this.cityName = cityName;
        this.ts = ts;
    }
}