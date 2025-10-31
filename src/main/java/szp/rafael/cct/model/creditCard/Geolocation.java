package szp.rafael.cct.model.creditCard;

import szp.rafael.cct.model.AbstractModel;

public class Geolocation extends AbstractModel {
    private double latitude;
    private double longitude;
    private String googlePlacesId;

    // Construtor padrão
    public Geolocation() {
    }

    // Construtor com todos os campos
    public Geolocation(double latitude, double longitude, String googlePlacesId) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.googlePlacesId = googlePlacesId;
    }

    // Getters e Setters
    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getGooglePlacesId() {
        return googlePlacesId;
    }

    public void setGooglePlacesId(String googlePlacesId) {
        this.googlePlacesId = googlePlacesId;
    }
}
