package com.dgusev.hlcup2018.accountsapp.parse;

import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import com.dgusev.hlcup2018.accountsapp.model.LikeRequest;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class LikeParser {

    public List<LikeRequest> parse(byte[] array) {
        if (array.length < 2) {
            throw new BadRequest();
        }
        List<LikeRequest> requests =  new ArrayList<>();
        int currentIndex = indexOf(array, 0, '[');
        while (true) {
            if (indexOf(array, currentIndex, '{') == -1) {
                break;
            }
            currentIndex = indexOf(array, currentIndex, '{');
            LikeRequest likeRequest = new LikeRequest();
            while (true) {
                if (array[currentIndex] == '}') {
                    break;
                }
                int fromField = indexOf(array, currentIndex, '"');
                int toField = indexOf(array, fromField + 1, '"');
                String subField = new String(array, fromField + 1, toField - fromField - 1 );
                if (subField.equals("likee")) {
                    int nextColon = indexOf(array, toField + 1, ':');
                    int nextComma = indexOf(array, nextColon + 1, ',');
                    int nextClose = indexOf(array, nextColon + 1, '}');
                    if (nextComma == -1) {
                        currentIndex = indexOf(array, nextColon + 1, '}');
                    } else {
                        if (nextClose < nextComma) {
                            currentIndex = nextClose;
                        } else {
                            currentIndex = nextComma;
                        }
                    }
                    int endIndex = currentIndex;
                    nextColon++;
                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                        nextColon++;
                    }
                    endIndex--;
                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                        endIndex--;
                    }
                    likeRequest.likee = Integer.parseInt(new String(array, nextColon, endIndex + 1 - nextColon));
                } else if (subField.equals("ts")) {
                    int nextColon = indexOf(array, toField + 1, ':');
                    int nextComma = indexOf(array, nextColon + 1, ',');
                    int nextClose = indexOf(array, nextColon + 1, '}');
                    if (nextComma == -1) {
                        currentIndex = indexOf(array, nextColon + 1, '}');
                    } else {
                        if (nextClose < nextComma) {
                            currentIndex = nextClose;
                        } else {
                            currentIndex = nextComma;
                        }
                    }
                    int endIndex = currentIndex;
                    nextColon++;
                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                        nextColon++;
                    }
                    endIndex--;
                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                        endIndex--;
                    }
                    likeRequest.ts = Integer.parseInt(new String(array, nextColon, endIndex + 1 - nextColon));
                } else if (subField.equals("liker")) {
                    int nextColon = indexOf(array, toField + 1, ':');
                    int nextComma = indexOf(array, nextColon + 1, ',');
                    int nextClose = indexOf(array, nextColon + 1, '}');
                    if (nextComma == -1) {
                        currentIndex = indexOf(array, nextColon + 1, '}');
                    } else {
                        if (nextClose < nextComma) {
                            currentIndex = nextClose;
                        } else {
                            currentIndex = nextComma;
                        }
                    }
                    int endIndex = currentIndex;
                    nextColon++;
                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                        nextColon++;
                    }
                    endIndex--;
                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                        endIndex--;
                    }
                    likeRequest.liker = Integer.parseInt(new String(array, nextColon, endIndex + 1 - nextColon));
                } else {
                    throw new BadRequest();
                }
            }
            requests.add(likeRequest);
        }
        return requests;
    }

    private int indexOf(byte[] array, int from, char ch) {
        for (int i = from; i < array.length; i++) {
            if (array[i] == ch) {
                return i;
            }
        }
        return -1;
    }
}
