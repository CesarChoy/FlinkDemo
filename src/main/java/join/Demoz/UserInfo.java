package join.Demoz;

/**
 * Create By 鸣宇淳 on 2020/6/4
 **/
public class UserInfo {
    private String userName;
    private Integer cityId;
    private Long ts;

    public UserInfo(){
    }

    public UserInfo(String userName, Integer cityId, Long ts) {
        this.userName = userName;
        this.cityId = cityId;
        this.ts = ts;
    }

    public String getUserName() {
        return userName;
    }

    public Integer getCityId() {
        return cityId;
    }

    public Long getTs() {
        return ts;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}