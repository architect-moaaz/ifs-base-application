package io.intelliflow.utils;


import java.io.Serializable;

public class CustomException extends Exception implements Serializable {

    private static final long serialVersionUID = 1L;

    private ResponseModel responseModel;

    public CustomException() {
        super();
    }

    public CustomException(String message, String status){
        this.responseModel = new ResponseModel(message,null,status);
    }

    public CustomException(String msg) {
        super(msg);
    }

    public CustomException(String msg, Exception e)  {
        super(msg, e);
    }

    public ResponseModel getResponseModel() {
        return responseModel;
    }
}

