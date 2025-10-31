package szp.rafael.cct.model.creditCard;

import szp.rafael.cct.model.AbstractModel;

public class IpData extends AbstractModel {

    private String publicIpAddress;

    // Construtor padrão
    public IpData() {
    }

    // Construtor com todos os campos
    public IpData(String publicIpAddress) {
        this.publicIpAddress = publicIpAddress;
    }

    // Getter e Setter
    public String getPublicIpAddress() {
        return publicIpAddress;
    }

    public void setPublicIpAddress(String publicIpAddress) {
        this.publicIpAddress = publicIpAddress;
    }

}
