package com.dgusev.hlcup2018.accountsapp.format;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

@Component
public class AccountFormatter {

    public String format(AccountDTO accountDTO, Set<String> fields) {
        StringBuilder stringBuilder = new StringBuilder("{");
        boolean first = true;
        for (String field: fields) {

            if (field.equals("id")) {
                if (!first) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\"").append(field).append("\":");
                stringBuilder.append(accountDTO.id);
                first = false;
            } else if (field.equals("email")) {
                if (!first) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\"").append(field).append("\":");
                stringBuilder.append("\"").append(accountDTO.email).append("\"");
                first = false;
            } else if (field.equals("fname")) {
                if (accountDTO.fname != null) {
                    if (!first) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"").append(field).append("\":");
                    stringBuilder.append("\"").append(accountDTO.fname).append("\"");
                    first = false;
                }
            } else if (field.equals("sname")) {
                if (accountDTO.sname != null) {
                    if (!first) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"").append(field).append("\":");
                    stringBuilder.append("\"").append(accountDTO.sname).append("\"");
                    first = false;
                }
            } else if (field.equals("phone")) {
                 if (accountDTO.phone != null) {
                     if (!first) {
                         stringBuilder.append(",");
                     }
                     stringBuilder.append("\"").append(field).append("\":");
                     stringBuilder.append("\"").append(accountDTO.phone).append("\"");
                     first = false;
                 }
            } else if (field.equals("sex")) {
                if (!first) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\"").append(field).append("\":");
                stringBuilder.append("\"").append(accountDTO.sex).append("\"");
                first = false;
            } else if (field.equals("birth")) {
                if (!first) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\"").append(field).append("\":");
                stringBuilder.append(accountDTO.birth);
                first = false;
            } else if (field.equals("country")) {
                if (accountDTO.country != null) {
                    if (!first) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"").append(field).append("\":");
                    stringBuilder.append("\"").append(accountDTO.country).append("\"");
                    first = false;
                }
            } else if (field.equals("city")) {
                if (accountDTO.city != null) {
                    if (!first) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"").append(field).append("\":");
                    stringBuilder.append("\"").append(accountDTO.city).append("\"");
                    first = false;
                }
            } else if (field.equals("joined")) {
                if (!first) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\"").append(field).append("\":");
                stringBuilder.append(accountDTO.joined);
                first = false;
            } else if (field.equals("status")) {
                if (!first) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\"").append(field).append("\":");
                stringBuilder.append("\"").append(accountDTO.status).append("\"");
                first = false;
            } else if (field.equals("interests")) {
                if (accountDTO.interests != null) {
                    if (!first) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"").append(field).append("\":");
                    stringBuilder.append("[");
                    stringBuilder.append(accountDTO.interests.stream().map(i -> "\"" + i + "\"").collect(Collectors.joining(",")));
                    stringBuilder.append("]");
                    first = false;
                }
            } else if (field.equals("premium")) {
                if (accountDTO.premiumStart != 0) {
                    if (!first) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"").append(field).append("\":");
                    stringBuilder.append("{\"start\":");
                    stringBuilder.append(accountDTO.premiumStart);
                    stringBuilder.append(",\"finish\":");
                    stringBuilder.append(accountDTO.premiumFinish);
                    stringBuilder.append("}");
                    first = false;
                }
            } else if (field.equals("likes")) {
                if (accountDTO.likes != null) {
                    if (!first) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("\"").append(field).append("\":");
                    stringBuilder.append("[");
                    stringBuilder.append(accountDTO.likes.stream().map(l -> "{\"id\":" +
                            l.id +
                            ",\"ts\":" +
                            l.ts +
                            "}"
                    ).collect(Collectors.joining(",")));
                    stringBuilder.append("]");
                    first = false;
                }
            }
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    public String formatRecommend(AccountDTO accountDTO) {
        StringBuilder stringBuilder = new StringBuilder("{\"id\":");
        stringBuilder.append(accountDTO.id).append(",\"email\":\"").append(accountDTO.email).append("\",\"status\":\"").append(accountDTO.status).append("\",\"birth\":").append(accountDTO.birth);
        if (accountDTO.fname != null) {
            stringBuilder.append(",\"fname\":\"").append(accountDTO.fname).append("\"");
        }
        if (accountDTO.sname != null) {
            stringBuilder.append(",\"sname\":\"").append(accountDTO.sname).append("\"");
        }
        if (accountDTO.premiumStart != 0) {
            stringBuilder.append(",");
            stringBuilder.append("\"").append("premium").append("\":");
            stringBuilder.append("{\"premiumStart\":");
            stringBuilder.append(accountDTO.premiumStart);
            stringBuilder.append(",\"premiumFinish\":");
            stringBuilder.append(accountDTO.premiumFinish);
            stringBuilder.append("}");
        }
        if (accountDTO.interests != null) {
            stringBuilder.append(",");
            stringBuilder.append("\"").append("interests").append("\":");
            stringBuilder.append("[");
            stringBuilder.append(accountDTO.interests.stream().map(i -> "\"" + i + "\"").collect(Collectors.joining(",")));
            stringBuilder.append("]");
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    public String formatSuggest(AccountDTO accountDTO) {
        StringBuilder stringBuilder = new StringBuilder("{\"id\":");
        stringBuilder.append(accountDTO.id).append(",\"email\":\"").append(accountDTO.email).append("\",\"status\":\"").append(accountDTO.status).append("\"");
        if (accountDTO.fname != null) {
            stringBuilder.append(",\"fname\":\"").append(accountDTO.fname).append("\"");
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

}
