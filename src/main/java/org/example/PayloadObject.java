package org.example;

public class PayloadObject {
    public String name;
    public String email;
    public String phone;
    public String id;

    public PayloadObject(String name, String email, String phone, String id) {
        this.name = name;
        this.email = email;
        this.phone = phone;
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "PayloadObject{" +
                "name='" + name + '\'' +
                ", email='" + email + '\'' +
                ", phone='" + phone + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
