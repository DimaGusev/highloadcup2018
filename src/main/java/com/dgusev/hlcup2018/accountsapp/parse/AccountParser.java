package com.dgusev.hlcup2018.accountsapp.parse;

import com.dgusev.hlcup2018.accountsapp.model.AccountDTO;
import com.dgusev.hlcup2018.accountsapp.model.BadRequest;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class AccountParser {

    private static final BadRequest BAD_REQUEST = new BadRequest();

    private static TLongObjectMap<String> parametersMap = new TLongObjectHashMap<>();

    static {
        List<String> values = Arrays.asList("id",
                "email",
                "fname",
                "sname",
                "phone",
                "sex",
                "birth",
                "country",
                "city",
                "status",
                "joined",
                "interests",
                "premium",
                "start",
                "finish",
                "likes",
                "id",
                "ts");
        for (String value:  values) {
            long hash = 0;
            for (int i = 0; i < value.length(); i++) {
                hash = 31* hash + (byte)value.charAt(i);
            }
            parametersMap.put(hash, value);
        }

    }

    private static final ThreadLocal<StringBuilder> parseStringBuilder = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder(100);
        }
    };

    private static final ThreadLocal<TLongArrayList> likesListPool = new ThreadLocal<TLongArrayList>() {
        @Override
        protected TLongArrayList initialValue() {
            return new TLongArrayList(50);
        }
    };

    public AccountDTO parse(byte[] array) {
        return parse(array, array.length);
    }

    public AccountDTO parse(byte[] array, int length) {
        return parse(array, 0, array.length);
    }

    public AccountDTO parse(byte[] array, int start, int length) {
        if (length - start < 2) {
            throw BAD_REQUEST;
        }
        AccountDTO accountDTO = new AccountDTO();
        int currentIndex = indexOf(array, start, length, '{');
        while (true) {
            if (array[currentIndex] == '}') {
                return accountDTO;
            }
            int fromField = indexOf(array, currentIndex, length, '"');
            int toField = indexOf(array, fromField + 1, length, '"');
            long fieldHash = calculateHash(array, fromField+1, toField - fromField - 1);
            String field = parametersMap.get(fieldHash);
            if (field == null) {
                throw BAD_REQUEST;
            }
            if (field.equals("id")) {
                int colon = indexOf(array, toField + 1, length, ':');
                int comma = indexOf(array, colon + 1, length, ',');
                int totalEnd;
                if (comma == -1) {
                    totalEnd = indexOf(array, colon + 1, length, '}');
                } else {
                    totalEnd = indexOf(array, colon + 1, length, ',');
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
                if (isNull(array, colon, end + 1 - colon)) {
                    throw BAD_REQUEST;
                } else {
                    accountDTO.id = decodeInt(array, colon, end + 1 - colon);
                }
                currentIndex = totalEnd;
            } else if (field.equals("email")) {
                int colon = indexOf(array, toField + 1, length, ':');
                int comma = indexOf(array, colon + 1, length, ',');
                int totalEnd;
                if (comma == -1) {
                    totalEnd = indexOf(array, colon + 1, length, '}');
                } else {
                    totalEnd = indexOf(array, colon + 1, length,  ',');
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
                if (isNull(array, colon, end + 1 - colon)) {
                    throw BAD_REQUEST;
                } else {
                    while (array[colon] == '"') {
                        colon++;
                    }
                    while (array[end] == '"') {
                        end--;
                    }
                    byte[] emailBytes = new byte[end + 1 - colon];
                    System.arraycopy(array, colon, emailBytes, 0, end + 1 - colon);
                    accountDTO.email = emailBytes;
                }
                currentIndex = totalEnd;
            } else if (field.equals("fname")) {
               int colon = indexOf(array, toField + 1, length, ':');
               int comma = indexOf(array, colon + 1, length, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length, ',');
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
               if (isNull(array, colon, end + 1 - colon)) {
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
               int colon = indexOf(array, toField + 1, length, ':');
               int comma = indexOf(array, colon + 1, length, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length, ',');
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
               if (isNull(array, colon, end + 1 - colon)) {
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
               int colon = indexOf(array, toField + 1, length, ':');
               int comma = indexOf(array, colon + 1, length, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length, ',');
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
               if (isNull(array, colon, end + 1 - colon)) {
                   accountDTO.phone = null;
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   byte[] phoneBytes = new byte[end + 1 - colon];
                   System.arraycopy(array, colon, phoneBytes, 0, end + 1 - colon);
                   accountDTO.phone = phoneBytes;
               }
               currentIndex = totalEnd;
           } else if (field.equals("sex")) {
               int colon = indexOf(array, toField + 1, length, ':');
               int comma = indexOf(array, colon + 1, length, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length, ',');
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
               if (isNull(array, colon, end + 1 - colon)) {
                   throw BAD_REQUEST;
               } else {
                   while (array[colon] == '"') {
                       colon++;
                   }
                   while (array[end] == '"') {
                       end--;
                   }
                   accountDTO.sex = parseString(array, colon, end + 1 - colon);
                   if (!accountDTO.sex.equals("m") && !accountDTO.sex.equals("f")) {
                       throw BAD_REQUEST;
                   }
               }
               currentIndex = totalEnd;
           } else if (field.equals("birth")) {
               int colon = indexOf(array, toField + 1, length, ':');
               int comma = indexOf(array, colon + 1, length, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length,'}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length, ',');
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
               if (isNull(array, colon, end + 1 - colon)) {
                   throw BAD_REQUEST;
               } else {
                   accountDTO.birth = decodeInt(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
           } else if (field.equals("country")) {
               int colon = indexOf(array, toField + 1, length, ':');
               int comma = indexOf(array, colon + 1, length, ',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length, '}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length, ',');
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
               if (isNull(array, colon, end + 1 - colon)) {
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
               int colon = indexOf(array, toField + 1, length, ':');
               int comma = indexOf(array, colon + 1, length,',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length,'}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length,',');
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
               if (isNull(array, colon, end + 1 - colon)) {
                   throw BAD_REQUEST;
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
               int colon = indexOf(array, toField + 1, length,':');
               int comma = indexOf(array, colon + 1, length,',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length,'}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length,',');
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
               if (isNull(array, colon, end + 1 - colon)) {
                   throw BAD_REQUEST;
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
               int colon = indexOf(array, toField + 1, length,':');
               int comma = indexOf(array, colon + 1, length,',');
               int totalEnd;
               if (comma == -1) {
                   totalEnd = indexOf(array, colon + 1, length,'}');
               } else {
                   totalEnd = indexOf(array, colon + 1, length,',');
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
               if (isNull(array, colon, end + 1 - colon)) {
                   throw BAD_REQUEST;
               } else {
                   accountDTO.joined = decodeInt(array, colon, end + 1 - colon);
               }
               currentIndex = totalEnd;
            } else if (field.equals("interests")) {
               int colon = indexOf(array, toField + 1, length,':');
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
                   List<String> interestsList = new ArrayList<>();
                   while (true) {
                       if (array[colon] == ']') {
                          break;
                       }
                       int fromInteres = indexOf(array, colon, length,'"');
                       int toInteres = indexOf(array, fromInteres + 1, length,'"');
                       String interes = parseString(array, fromInteres + 1, toInteres - fromInteres - 1 );
                       interestsList.add(interes);
                       int commaIndex = indexOf(array, toInteres, length,',');
                       int closeIndex = indexOf(array, toInteres, length,']');
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
                   accountDTO.interests = interestsList.toArray(new String[interestsList.size()]);
                   int nextIndex = indexOf(array, colon, length,',');
                   if (nextIndex == -1) {
                       currentIndex = indexOf(array, colon, length,'}');
                   } else {
                       currentIndex = indexOf(array, colon, length,',');
                   }
               }

                } else if (field.equals("premium")) {
                    int colon = indexOf(array, toField + 1, length,':');
                    while (array[colon] != '{') {
                        if (array[colon] == '"') {
                            throw BAD_REQUEST;
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
                            int fromSubField = indexOf(array, colon, length,'"');
                            int toSubField = indexOf(array, fromSubField + 1, length,'"');
                            long subFieldHash = calculateHash(array, fromSubField + 1, toSubField - fromSubField - 1);
                            String subField = parametersMap.get(subFieldHash);
                            if (subField == null) {
                                throw BAD_REQUEST;
                            }
                            if (subField.equals("start")) {
                                int nextColon = indexOf(array, toSubField + 1, length,':');
                                int nextComma = indexOf(array, nextColon + 1, length,',');
                                int nextClose = indexOf(array, nextColon + 1, length,'}');
                                if (nextComma == -1) {
                                    colon = indexOf(array, nextColon + 1, length,'}');
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
                                accountDTO.premiumStart = decodeInt(array, nextColon, endIndex + 1 - nextColon);
                            } else if (subField.equals("finish")) {
                                int nextColon = indexOf(array, toSubField + 1, length,':');
                                int nextComma = indexOf(array, nextColon + 1, length,',');
                                int nextClose = indexOf(array, nextColon + 1, length,'}');
                                if (nextComma == -1) {
                                    colon = indexOf(array, nextColon + 1, length,'}');
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
                                accountDTO.premiumFinish = decodeInt(array, nextColon, endIndex + 1 - nextColon);
                            } else {
                                throw BAD_REQUEST;
                            }

                        }
                        int nextIndex = indexOf(array, colon, length,',');
                        if (nextIndex == -1) {
                            currentIndex = indexOf(array, colon, length,'}');
                        } else {
                            currentIndex = indexOf(array, colon, length,',');
                        }
                    }

                } else if (field.equals("likes")) {
                    int colon = indexOf(array, toField + 1, length,':');
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
                        TLongArrayList likesList = likesListPool.get();
                        likesList.reset();
                        while (true) {
                            if (colon < 0) {
                                int i = 100;
                            }
                            if (array[colon] == ']') {
                                break;
                            }
                            int fromLike = indexOf(array, colon, length,'{');
                            int toLike = indexOf(array, fromLike + 1, length,'}');
                            colon = fromLike;

                            long like = 0;
                            while (true) {
                                if (array[colon] == '}') {
                                    break;
                                }
                                int fromSubField = indexOf(array, colon, length,'"');
                                int toSubField = indexOf(array, fromSubField + 1, length,'"');
                                long subFieldHash = calculateHash(array, fromSubField + 1, toSubField - fromSubField - 1);
                                String subfield = parametersMap.get(subFieldHash);
                                if (subfield == null) {
                                    throw BAD_REQUEST;
                                }
                                if (subfield.equals("id")) {
                                    int nextColon = indexOf(array, toSubField + 1, length,':');
                                    int nextComma = indexOf(array, nextColon + 1, length,',');
                                    int nextClose = indexOf(array, nextColon + 1, length,'}');
                                    if (nextComma == -1) {
                                        colon = indexOf(array, nextColon + 1, length,'}');
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
                                    like = like & 0xffffffffL;
                                    like = like | (long)decodeInt(array, nextColon, endIndex + 1 - nextColon) << 32;
                                } else if (subfield.equals("ts")) {
                                    int nextColon = indexOf(array, toSubField + 1, length,':');
                                    int nextComma = indexOf(array, nextColon + 1, length,',');
                                    int nextClose = indexOf(array, nextColon + 1, length,'}');
                                    if (nextComma == -1) {
                                        colon = indexOf(array, nextColon + 1, length,'}');
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
                                    like = like & 0xffffffff00000000L;
                                    like = like | decodeInt(array, nextColon, endIndex + 1 - nextColon);
                                }
                            }
                            likesList.add(like);

                            int commaIndex = indexOf(array, toLike, length,',');
                            int closeIndex = indexOf(array, toLike, length,']');
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
                        long[] arr = new long[likesList.size()];
                        for (int i = 0; i < likesList.size(); i++) {
                            arr[i] = likesList.get(i);
                        }
                        Arrays.sort(arr);
                        reverse(arr);
                        accountDTO.likes = arr;
                        int nextIndex = indexOf(array, colon, length,',');
                        if (nextIndex == -1) {
                            currentIndex = indexOf(array, colon, length,'}');
                        } else {
                            currentIndex = indexOf(array, colon, length,',');
                        }
                    }

           } else {
                currentIndex++;
            }
        }

    }

    private int indexOf(byte[] array, int from, int to, char ch) {
        for (int i = from; i < to; i++) {
            if (array[i] == ch) {
                return i;
            }
        }
        return -1;
    }

    private void reverse(long[] array) {
        int size = array.length;
        int half = size / 2;
        for (int i = 0; i < half; i++) {
            long tmp = array[i];
            array[i] = array[size - 1 - i];
            array[size - 1 - i] = tmp;
        }
    }

    private static String parseString(byte[] buf, int start, int count) {
        StringBuilder stringBuilder = parseStringBuilder.get();
        stringBuilder.setLength(0);
        int index = 0;
        while (index < count) {
            if (buf[start + index] == '\\') {
                stringBuilder.append((char) decodeHexInt(buf, start + index + 2, 4));
                index += 6;
            } else {
                stringBuilder.append((char)buf[start + index]);
                index++;
            }
        }
        return stringBuilder.toString();
    }

    private static final int POW10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
    private static final int POW16[] = {1, 16, 256, 4096};


    private int decodeInt(byte[] buf, int from, int length) {
        if (length > 10) {
            throw BAD_REQUEST;
        }
        int result = 0;
        for (int i = from; i < from + length; i++) {
            int value = buf[i] - 48;
            if (value <0 || value > 9) {
                throw BAD_REQUEST;
            }
            result+=POW10[length - (i - from) - 1]* value;
        }
        return result;
    }

    private boolean isNull(byte[] array, int from, int length) {
        if (length != 4) {
            return false;
        }
        if (array[from] != 'n' || array[from + 1] != 'u' || array[from + 2] != 'l' || array[from + 3] != 'l') {
            return false;
        }
        return true;
    }

    private long calculateHash(byte[] array, int from, int length) {
        long result = 0;
        for (int i = from; i < from + length; i++) {
            result =31* result + array[i];
        }
        return result;
    }


    private static int decodeHexInt(byte[] buf, int from, int length) {
        int result = 0;
        for (int i = 0; i < length; i++) {
            result+=POW16[i] * convert(buf[from + length - 1 - i]);
        }
        return result;
    }

    private static int convert(byte ch) {
        switch (ch) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'a':
                return 10;
            case 'b':
                return 11;
            case 'c':
                return 12;
            case 'd':
                return 13;
            case 'e':
                return 14;
            case 'f':
                return 15;
        }
        return 0;
    }

}
