package szp.rafael.cct.model.creditCard;

import szp.rafael.cct.model.AbstractModel;

public class CardDetails extends AbstractModel {


        private String cardId;
        private String nameOnCard; // Nome estampado no cartão
        private String brand;      // Bandeira (ex: Visa, Mastercard)
        private String expiryDate; // Validade (ex: "MM/YY")

        // Construtor padrão (necessário para serialização/desserialização do Kafka Streams)
        public CardDetails() {
        }

        // Construtor com todos os campos
        public CardDetails(String cardId, String nameOnCard, String brand, String expiryDate) {
            this.cardId = cardId;
            this.nameOnCard = nameOnCard;
            this.brand = brand;
            this.expiryDate = expiryDate;
        }

        // Getters e Setters
        public String getCardId() {
            return cardId;
        }

        public void setCardId(String cardId) {
            this.cardId = cardId;
        }

        public String getNameOnCard() {
            return nameOnCard;
        }

        public void setNameOnCard(String nameOnCard) {
            this.nameOnCard = nameOnCard;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public String getExpiryDate() {
            return expiryDate;
        }

        public void setExpiryDate(String expiryDate) {
            this.expiryDate = expiryDate;
        }



}
