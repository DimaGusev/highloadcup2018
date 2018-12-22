package com.dgusev.hlcup2018.accountsapp.parse;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;

@Component
public class AccountParser {

    private static Field fieldSB;

    static {
        try {
            fieldSB = Class.forName("java.lang.AbstractStringBuilder").getDeclaredField("value");
            fieldSB.setAccessible(true);
        } catch (NoSuchFieldException e) {

        } catch (ClassNotFoundException e) {
        }
    }


    public AccountDTO parse(byte[] array) {
        if (array.length < 2) {
            throw new BadRequest();
        }
        AccountDTO accountDTO = new AccountDTO();
        int currentIndex = indexOf(array, 0, '{');
        while (true) {
            if (array[currentIndex] == '}') {
                return accountDTO;
            }
            int fromField = indexOf(array, currentIndex, '"');
            int toField = indexOf(array, fromField + 1, '"');
            String field = new String(array, fromField+1, toField - fromField - 1);
            if (field.equals("id")) {
                int colon = indexOf(array, toField + 1, ':');
                int comma = indexOf(array, colon + 1, ',');
                int totalEnd;
                if (comma == -1) {
                    totalEnd = indexOf(array, colon + 1, '}');
                } else {
                    totalEnd = indexOf(array, colon + 1, ',');
                }
                int end = totalEnd;
                colon++;
                while (array[colon] == ' ' || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
                if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                    throw new IllegalArgumentException("id is null");
                } else {
                    accountDTO.id = Integer.parseInt(new String(array, colon, end + 1 - colon));
                }
                currentIndex = totalEnd;
            } else if (field.equals("email")) {
                int colon = indexOf(array, toField + 1, ':');
                int comma = indexOf(array, colon + 1, ',');
                int totalEnd;
                if (comma == -1) {
                    totalEnd = indexOf(array, colon + 1, '}');
                } else {
                    totalEnd = indexOf(array, colon + 1, ',');
                }
                int end = totalEnd;
                colon++;
                while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
                if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                    throw new IllegalArgumentException("email is null");
                } else {
                    while (array[colon] == '"') {
                        colon++;
                    }
                    while (array[end] == '"') {
                        end--;
                    }
                    accountDTO.email = new String(array, colon, end + 1 - colon);
                }
                currentIndex = totalEnd;
            } else if (field.equals("fname")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
               colon++;
               while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                   colon++;
               }
               end--;
               while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                   end--;
               }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   accountDTO.fname = null;
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.fname = parseString(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("sname")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
               colon++;
               while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                   colon++;
               }
               end--;
               while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                   end--;
               }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   accountDTO.sname = null;
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.sname = parseString(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("phone")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
               colon++;
               while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                   colon++;
               }
               end--;
               while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                   end--;
               }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   accountDTO.phone = null;
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.phone = parseString(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("sex")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
                colon++;
                while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   throw new IllegalArgumentException("sex is null");
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.sex = parseString(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("birth")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
                colon++;
                while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   throw new IllegalArgumentException("birth is null");
               } else {
                   accountDTO.birth = Integer.parseInt(new String(array, colon, end + 1 - colon));
               }
               currentIndex = totalEnd;
           } else if (field.equals("country")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
                colon++;
                while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   accountDTO.country = null;
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.country = parseString(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("city")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
                colon++;
                while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   throw new IllegalArgumentException("sex is null");
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.city = parseString(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("status")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
                colon++;
                while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   throw new IllegalArgumentException("status is null");
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.status = parseString(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("joined")) {
               int colon = indexOf(array, toField + 1, ':');
               int comma = indexOf(array, colon + 1, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, ',');
               }
               int end = totalEnd;
                colon++;
                while (array[colon] == ' '  || array[colon] == '\r' || array[colon] == '\n') {
                    colon++;
                }
                end--;
                while (array[end] == ' ' || array[end] == '\r' || array[end] == '\n') {
                    end--;
                }
               if (end - colon == 3 && new String(array, colon, end + 1 - colon).equals("null")) {
                   throw new IllegalArgumentException("joined is null");
               } else {
                   accountDTO.joined = Integer.parseInt(new String(array, colon, end + 1 - colon));
               }
               currentIndex = totalEnd;
            } else if (field.equals("interests")) {
               int colon = indexOf(array, toField + 1, ':');
               while (array[colon] != '[') {
                   colon++;
               }
               int end = colon;
               while (array[end] != ']') {
                   end++;
               }
               if (end - colon == 1) {
                   accountDTO.interests = null;
               } else {
                   accountDTO.interests = new ArrayList<>();
                   while (true) {
                       if (array[colon] == ']') {
                          break;
                       }
                       int fromInteres = indexOf(array, colon, '"');
                       int toInteres = indexOf(array, fromInteres + 1, '"');
                       String interes = parseString(array, fromInteres + 1, toInteres - fromInteres - 1 );
                       accountDTO.interests.add(interes);
                       int commaIndex = indexOf(array, toInteres, ',');
                       int closeIndex = indexOf(array, toInteres, ']');
                       if (commaIndex == -1) {
                           colon = closeIndex;
                       } else {
                           if (closeIndex < commaIndex) {
                               colon = closeIndex;
                           } else {
                               colon = commaIndex;
                           }
                       }
                   }
                   int nextIndex = indexOf(array, colon, ',');
                   if (nextIndex == -1) {
                       currentIndex = indexOf(array, colon, '}');
                   } else {
                       currentIndex = indexOf(array, colon, ',');
                   }
               }

                } else if (field.equals("premium")) {
                    int colon = indexOf(array, toField + 1, ':');
                    while (array[colon] != '{') {
                        if (array[colon] == '"') {
                            throw new BadRequest();
                        }
                        colon++;
                    }
                    int end = colon;
                    while (array[end] != '}') {
                        end++;
                    }
                    if (end - colon == 1) {
                        accountDTO.premiumStart = 0;
                        accountDTO.premiumFinish = 0;
                    } else {
                        while (true) {
                            if (array[colon] == '}') {
                                break;
                            }
                            int fromSubField = indexOf(array, colon, '"');
                            int toSubField = indexOf(array, fromSubField + 1, '"');
                            String subField = new String(array, fromSubField + 1, toSubField - fromSubField - 1);
                            if (subField.equals("start")) {
                                int nextColon = indexOf(array, toSubField + 1, ':');
                                int nextComma = indexOf(array, nextColon + 1, ',');
                                int nextClose = indexOf(array, nextColon + 1, '}');
                                if (nextComma == -1) {
                                    colon = indexOf(array, nextColon + 1, '}');
                                } else {
                                    if (nextClose < nextComma) {
                                        colon = nextClose;
                                    } else {
                                        colon = nextComma;
                                    }
                                }
                                int endIndex = colon;
                                nextColon++;
                                while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                                    nextColon++;
                                }
                                endIndex--;
                                while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                                    endIndex--;
                                }
                                accountDTO.premiumStart = Integer.parseInt(new String(array, nextColon, endIndex + 1 - nextColon));
                            } else if (subField.equals("finish")) {
                                int nextColon = indexOf(array, toSubField + 1, ':');
                                int nextComma = indexOf(array, nextColon + 1, ',');
                                int nextClose = indexOf(array, nextColon + 1, '}');
                                if (nextComma == -1) {
                                    colon = indexOf(array, nextColon + 1, '}');
                                } else {
                                    if (nextClose < nextComma) {
                                        colon = nextClose;
                                    } else {
                                        colon = nextComma;
                                    }
                                }
                                int endIndex = colon;
                                nextColon++;
                                while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                                    nextColon++;
                                }
                                endIndex--;
                                while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                                    endIndex--;
                                }
                                accountDTO.premiumFinish = Integer.parseInt(new String(array, nextColon, endIndex + 1 - nextColon));
                            } else {
                                throw new BadRequest();
                            }

                        }
                        int nextIndex = indexOf(array, colon, ',');
                        if (nextIndex == -1) {
                            currentIndex = indexOf(array, colon, '}');
                        } else {
                            currentIndex = indexOf(array, colon, ',');
                        }
                    }

                } else if (field.equals("likes")) {
                    int colon = indexOf(array, toField + 1, ':');
                    while (array[colon] != '[') {
                        colon++;
                    }
                    int end = colon;
                    while (array[end] != ']') {
                        end++;
                    }
                    if (end - colon == 1) {
                        accountDTO.likes = null;
                    } else {
                        accountDTO.likes = new ArrayList<>();

                        while (true) {
                            if (array[colon] == ']') {
                                break;
                            }
                            int fromLike = indexOf(array, colon, '{');
                            int toLike = indexOf(array, fromLike + 1, '}');
                            colon = fromLike;

                            AccountDTO.Like like = new AccountDTO.Like();
                            while (true) {
                                if (array[colon] == '}') {
                                    break;
                                }
                                int fromSubField = indexOf(array, colon, '"');
                                int toSubField = indexOf(array, fromSubField + 1, '"');
                                String subfield = new String(array, fromSubField + 1, toSubField - fromSubField - 1);
                                if (subfield.equals("id")) {
                                    int nextColon = indexOf(array, toSubField + 1, ':');
                                    int nextComma = indexOf(array, nextColon + 1, ',');
                                    int nextClose = indexOf(array, nextColon + 1, '}');
                                    if (nextComma == -1) {
                                        colon = indexOf(array, nextColon + 1, '}');
                                    } else {
                                        if (nextClose < nextComma) {
                                            colon = nextClose;
                                        } else {
                                            colon = nextComma;
                                        }
                                    }
                                    int endIndex = colon;
                                    nextColon++;
                                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                                        nextColon++;
                                    }
                                    endIndex--;
                                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                                        endIndex--;
                                    }
                                    like.id = Integer.parseInt(new String(array, nextColon, endIndex + 1 - nextColon));
                                } else if (subfield.equals("ts")) {
                                    int nextColon = indexOf(array, toSubField + 1, ':');
                                    int nextComma = indexOf(array, nextColon + 1, ',');
                                    int nextClose = indexOf(array, nextColon + 1, '}');
                                    if (nextComma == -1) {
                                        colon = indexOf(array, nextColon + 1, '}');
                                    } else {
                                        if (nextClose < nextComma) {
                                            colon = nextClose;
                                        } else {
                                            colon = nextComma;
                                        }
                                    }
                                    int endIndex = colon;
                                    nextColon++;
                                    while (array[nextColon] == ' ' || array[nextColon] == '\r' || array[nextColon] == '\n') {
                                        nextColon++;
                                    }
                                    endIndex--;
                                    while (array[endIndex] == ' ' || array[endIndex] == '\r' || array[endIndex] == '\n') {
                                        endIndex--;
                                    }
                                    like.ts = Integer.parseInt(new String(array, nextColon, endIndex + 1 - nextColon));
                                }
                            }
                            accountDTO.likes.add(like);

                            int commaIndex = indexOf(array, toLike, ',');
                            int closeIndex = indexOf(array, toLike, ']');
                            if (commaIndex == -1) {
                                colon = closeIndex;
                            } else {
                                if (closeIndex < commaIndex) {
                                    colon = closeIndex;
                                } else {
                                    colon = commaIndex;
                                }
                            }
                        }
                        int nextIndex = indexOf(array, colon, ',');
                        if (nextIndex == -1) {
                            currentIndex = indexOf(array, colon, '}');
                        } else {
                            currentIndex = indexOf(array, colon, ',');
                        }
                    }

           } else {
                currentIndex++;
            }
        }

    }

    private int indexOf(byte[] array, int from, char ch) {
        for (int i = from; i < array.length; i++) {
            if (array[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    private static String parseString(byte[] buf, int start, int count) {
        StringBuilder stringBuilder = new StringBuilder(count / 6 + 1);
        int index = 0;
        while (index < count) {
            if (buf[start + index] == '\\') {
                stringBuilder.append((char) Integer.parseInt(new String(buf, start + index + 2, 4), 16));
                index += 6;
            } else {
                stringBuilder.append((char)buf[start + index]);
                index++;
            }
        }
        return stringBuilder.toString();
    }

    private static byte[] getValue(StringBuilder s) {
        try {
            return ((byte[]) fieldSB.get(s));
        } catch (IllegalAccessException e) {
            return null;
        }
    }
}
