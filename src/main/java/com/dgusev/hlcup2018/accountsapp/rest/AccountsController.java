package com.dgusev.hlcup2018.accountsapp.rest;

import com.dgusev.hlcup2018.accountsapp.format.AccountFormatter;
import com.dgusev.hlcup2018.accountsapp.format.GroupFormatter;
import com.dgusev.hlcup2018.accountsapp.index.IndexHolder;
import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.*;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import com.dgusev.hlcup2018.accountsapp.parse.LikeParser;
import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.predicate.*;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import com.dgusev.hlcup2018.accountsapp.service.ConvertorUtills;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.netty.buffer.ByteBuf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Component
public class AccountsController {

    private static final byte[] EMPTY_ACCOUNTS_LIST = "{\"accounts\": []}".getBytes();
    private static final byte[] ACCOUNTS_LIST_START = "{\"accounts\": [".getBytes();
    private static final byte[] LIST_END = "]}".getBytes();
    private static final byte[] EMPTY_GROUPS_LIST = "{\"groups\": []}".getBytes();
    private static final byte[] GROUPS_LIST_START = "{\"groups\": [".getBytes();

    @Autowired
    private AccountService accountService;

    @Autowired
    private AccountFormatter accountFormatter;

    @Autowired
    private GroupFormatter groupFormatter;

    @Autowired
    private AccountParser accountParser;

    @Autowired
    private LikeParser likeParser;

    @Autowired
    private Dictionary dictionary;

    @Autowired
    private IndexHolder indexHolder;

    private static final ThreadLocal<List<Predicate<Account>>> predicateList = new ThreadLocal<List<Predicate<Account>>>() {
        @Override
        protected List<Predicate<Account>> initialValue() {
            return new ArrayList<>(10);
        }
    };

    private static final ThreadLocal<List<String>> fieldsList = new ThreadLocal<List<String>>() {
        @Override
        protected List<String> initialValue() {
            return new ArrayList<>(10);
        }
    };

    public int accountsFilter(Map<String, String> allRequestParams, byte[] responseBuf) throws Exception {
        List<Predicate<Account>> predicates = predicateList.get();
        predicates.clear();
        int limit = 0;
        List<String> fields = fieldsList.get();
        fields.clear();
        fields.add("id");
        fields.add("email");
        int predicateMask = 0;
        boolean empty = false;
        for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
            String name = parameter.getKey();
            if (name.equals("query_id")) {
                continue;
            }
            if (name.equals("limit")) {
                limit = Integer.parseInt(parameter.getValue());
                continue;
            }
            String field = name.substring(0, name.indexOf("_"));
            if (!fields.contains(field)) {
                fields.add(field);
            }
            if (name.startsWith("sex_")) {
                if (name.equals("sex_eq")) {
                    predicates.add(new SexEqPredicate(ConvertorUtills.convertSex(parameter.getValue())));
                    predicateMask |=2;
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("email_")) {
                if (name.equals("email_domain")) {
                    predicates.add(new EmailDomainPredicate(parameter.getValue()));
                } else if (name.equals("email_lt")) {
                    String value = parameter.getValue();
                    if (compareTo(value, indexHolder.minEmail) < 0) {
                        empty = true;
                    }
                    predicates.add(new EmailLtPredicate(value));
                } else if (name.equals("email_gt")) {
                    predicates.add(new EmailGtPredicate(parameter.getValue()));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("status_")) {
                if (name.equals("status_eq")) {
                    predicates.add(new StatusEqPredicate(ConvertorUtills.convertStatusNumber(parameter.getValue())));
                } else if (name.equals("status_neq")) {
                    predicates.add(new StatusNEqPredicate(ConvertorUtills.convertStatusNumber(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }

            } else if (name.startsWith("fname_")) {
                if (name.equals("fname_eq")) {
                    predicates.add(new FnameEqPredicate(dictionary.getFname(parameter.getValue())));
                } else if (name.equals("fname_any")) {
                    String[] fnames = parameter.getValue().split(",");
                    int[] values = new int[fnames.length];
                    for (int i = 0; i < fnames.length; i++) {
                        values[i] = dictionary.getFname(fnames[i]);
                    }
                    predicates.add(new FnameAnyPredicate(values));
                    predicateMask |=1;
                } else if (name.equals("fname_null")) {
                    predicates.add(new FnameNullPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("sname_")) {
                if (name.equals("sname_eq")) {
                    predicates.add(new SnameEqPredicate(dictionary.getFname(parameter.getValue())));
                } else if (name.equals("sname_starts")) {
                    predicates.add(new SnameStartsPredicate(parameter.getValue(), dictionary));
                } else if (name.equals("sname_null")) {
                    predicates.add(new SnameNullPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("phone_")) {
                if (name.equals("phone_code")) {
                    predicates.add(new PhoneCodePredicate(parameter.getValue()));
                } else if (name.equals("phone_null")) {
                    predicates.add(new PhoneNullPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("country_")) {
                if (name.equals("country_eq")) {
                    byte countryIndex = dictionary.getCountry(parameter.getValue());
                    predicates.add(new CountryEqPredicate(countryIndex));
                } else if (name.equals("country_null")) {
                    predicates.add(new CountryNullPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("city_")) {
                if (name.equals("city_eq")) {
                    predicates.add(new CityEqPredicate(dictionary.getCity(parameter.getValue())));
                } else if (name.equals("city_any")) {
                    String[] cities = parameter.getValue().split(",");
                    int[] values = new int[cities.length];
                    for (int i = 0; i < cities.length; i++) {
                        values[i] = dictionary.getCity(cities[i]);
                    }
                    predicates.add(new CityAnyPredicate(values));
                } else if (name.equals("city_null")) {
                    predicates.add(new CityNullPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("birth_")) {
                if (name.equals("birth_lt")) {
                    predicates.add(new BirthLtPredicate(Integer.parseInt(parameter.getValue())));
                } else if (name.equals("birth_gt")) {
                    predicates.add(new BirthGtPredicate(Integer.parseInt(parameter.getValue())));
                } else if (name.equals("birth_year")) {
                    predicates.add(new BirthYearPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("interests_")) {
                if (name.equals("interests_contains")) {
                    String[] interests = parameter.getValue().split(",");
                    byte[] values = new byte[interests.length];
                    for (int i = 0; i < interests.length; i++) {
                        values[i] = dictionary.getInteres(interests[i]);
                    }
                    predicates.add(new InterestsContainsPredicate(values));
                } else if (name.equals("interests_any")) {
                    String[] interests = parameter.getValue().split(",");
                    byte[] values = new byte[interests.length];
                    for (int i = 0; i < interests.length; i++) {
                        values[i] = dictionary.getInteres(interests[i]);
                    }
                    predicates.add(new InterestsAnyPredicate(values));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("likes_")) {
                if (name.equals("likes_contains")) {
                    predicates.add(new LikesContainsPredicate(Arrays.stream(parameter.getValue().split(",")).mapToInt(Integer::parseInt).toArray()));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("premium_")) {
                if (name.equals("premium_now")) {
                    predicates.add(new PremiumNowPredicate());
                } else if (name.equals("premium_null")) {
                    predicates.add(new PremiumNullPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw new BadRequest();
                }
            } else {
                throw BadRequest.INSTANCE;
            }
        }
        if (empty) {
            System.arraycopy(EMPTY_ACCOUNTS_LIST, 0, responseBuf, 0, EMPTY_ACCOUNTS_LIST.length);
            return EMPTY_ACCOUNTS_LIST.length;
        }
        List<Account> result = accountService.filter(predicates, limit, predicateMask);
        if (result.isEmpty()) {
            System.arraycopy(EMPTY_ACCOUNTS_LIST, 0, responseBuf, 0, EMPTY_ACCOUNTS_LIST.length);
            return EMPTY_ACCOUNTS_LIST.length;
        } else {
            int index = 0;
            System.arraycopy(ACCOUNTS_LIST_START, 0, responseBuf, 0, ACCOUNTS_LIST_START.length);
            index+=ACCOUNTS_LIST_START.length;
            for (int i = 0; i < result.size(); i++) {
                if (i != 0) {
                    responseBuf[index++] = ',';
                }
                index = accountFormatter.format(result.get(i), fields, responseBuf, index);
            }
            System.arraycopy(LIST_END, 0, responseBuf, index, LIST_END.length);
            index+=LIST_END.length;
            return index;
        }
    }

    private int compareTo(String values1, byte[] values2) {
        int len1 = values1.length();
        int len2 = values2.length;
        int lim = 0;
        if (len1 < len2) {
            lim = len1;
        } else {
            lim = len2;
        }
        int k = 0;
        while (k < lim) {
            byte c1 = (byte) values1.charAt(k);
            byte c2 = values2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }

    public int group(Map<String,String> allRequestParams, byte[] responseBuf) {
            List<String> keys = new ArrayList<>();
            int order = 1;
            int limit = 0;
            List<Predicate<Account>> predicates = new ArrayList<>();
            byte keysMask = 0;
            byte predicatesMask = 0;
            boolean sex = false;
            byte status = -1;
            byte country = -1;
            int city = -1;
            int birthYear = -1;
            byte interes = -1;
            int like = -1;
            int joinedYear = -1;
            for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                }

                if (name.equals("keys")) {
                    keys.addAll(Arrays.asList(parameter.getValue().split(",")));

                    if (keys.contains("interests")) {
                        keys.remove("interests");
                        keys.add("interests");
                    }
                    for (String key: keys) {
                        if (key.equals("sex")) {
                            keysMask|=1;
                        } else if (key.equals("status")) {
                            keysMask|=1<<1;
                        } else if (key.equals("interests")) {
                            keysMask|=1<<2;
                        } else if (key.equals("country")) {
                            keysMask|=1<<3;
                        } else if (key.equals("city")) {
                            keysMask|=1<<4;
                        } else {
                            throw new BadRequest();
                        }
                    }

                } else if (name.equals("order")) {
                    order = Integer.parseInt(parameter.getValue());
                } else if (name.equals("limit")) {
                    limit = Integer.parseInt(parameter.getValue());
                } else if (name.equals("sex")) {
                    predicatesMask|=1;
                    sex = ConvertorUtills.convertSex(parameter.getValue());
                } else if (name.equals("status")) {
                    predicatesMask|=1 << 1;
                    status = ConvertorUtills.convertStatusNumber(parameter.getValue());
                } else if (name.equals("country")) {
                    predicatesMask|=1 << 2;
                    country = dictionary.getCountry(parameter.getValue());
                } else if (name.equals("city")) {
                    predicatesMask|=1 << 3;
                    city = dictionary.getCity(parameter.getValue());
                } else if (name.equals("birth")) {
                    predicatesMask|=1 << 4;
                    birthYear = Integer.parseInt(parameter.getValue());
                } else if (name.equals("interests")) {
                    predicatesMask|=1 << 5;
                    interes = dictionary.getInteres(parameter.getValue());
                } else if (name.equals("likes")) {
                    predicatesMask|=1 << 6;
                    like = Integer.parseInt(parameter.getValue());
                } else if (name.equals("joined")) {
                    predicatesMask|=1 << 7;
                    joinedYear = Integer.parseInt(parameter.getValue());
                } else {
                    throw new BadRequest();
                }
            }

            List<Group> groups = accountService.group(keys, sex, status, country, city, birthYear, interes, like, joinedYear, order, limit, keysMask, predicatesMask);
            if (groups.isEmpty()) {
                System.arraycopy(EMPTY_GROUPS_LIST, 0, responseBuf, 0, EMPTY_GROUPS_LIST.length);
                return EMPTY_GROUPS_LIST.length;
            } else {
                int index = 0;
                System.arraycopy(GROUPS_LIST_START, 0, responseBuf, 0, GROUPS_LIST_START.length);
                index+=GROUPS_LIST_START.length;
                for (int i = 0; i < groups.size(); i++) {
                    if (i != 0) {
                        responseBuf[index++] = ',';
                    }
                    index = groupFormatter.format(groups.get(i), keys, responseBuf, index);
                }
                System.arraycopy(LIST_END, 0, responseBuf, index, LIST_END.length);
                index+=LIST_END.length;
                return index;
            }
    }

    public int recommend(Map<String, String> allRequestParams, int id, byte[] responseBuf) {
            if (id >= AccountService.MAX_ID || accountService.findById(id) == null) {
                throw new NotFoundRequest();
            }
            int limit = 0;
            byte country = -1;
            int city = -1;
            for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                } else if (name.equals("limit")) {
                    limit = Integer.parseInt(parameter.getValue());
                    if (limit < 0) {
                        throw new BadRequest();
                    }
                } else if (name.equals("country")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw new BadRequest();
                    }
                    country = dictionary.getCountry(parameter.getValue());
                } else if (name.equals("city")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw new BadRequest();
                    }
                    city = dictionary.getCity(parameter.getValue());
                } else {
                    throw new BadRequest();
                }
            }

            List<Account> result = accountService.recommend(id, country, city, limit);
            if (result.isEmpty()) {
                System.arraycopy(EMPTY_ACCOUNTS_LIST, 0, responseBuf, 0, EMPTY_ACCOUNTS_LIST.length);
                return EMPTY_ACCOUNTS_LIST.length;
            } else {
                int index = 0;
                System.arraycopy(ACCOUNTS_LIST_START, 0, responseBuf, 0, ACCOUNTS_LIST_START.length);
                index+=ACCOUNTS_LIST_START.length;
                for (int i = 0; i < result.size(); i++) {
                    if (i != 0) {
                        responseBuf[index++] = ',';
                    }
                    index = accountFormatter.formatRecommend(result.get(i), responseBuf, index);
                }
                System.arraycopy(LIST_END, 0, responseBuf, index, LIST_END.length);
                index+=LIST_END.length;
                return index;
            }
    }


    public int suggest(Map<String,String> allRequestParams, int id, byte[] responseBuf) {
        if (id >= AccountService.MAX_ID || accountService.findById(id) == null) {
            throw new NotFoundRequest();
        }
        int limit = 0;
        List<Predicate<Account>> predicates = new ArrayList<>();
        for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
            String name = parameter.getKey();
            if (name.equals("query_id")) {
                continue;
            } else if (name.equals("limit")) {
                limit = Integer.parseInt(parameter.getValue());
                if (limit <= 0) {
                    throw new BadRequest();
                }
            } else if (name.equals("country")) {
                if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                    throw new BadRequest();
                }
                predicates.add(new CountryEqPredicate(dictionary.getCountry(parameter.getValue())));
            } else if (name.equals("city")) {
                if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                    throw new BadRequest();
                }
                predicates.add(new CityEqPredicate(dictionary.getCity(parameter.getValue())));
            } else {
                throw new BadRequest();
            }
        }

        List<Account> result = accountService.suggest(id, predicates, limit);
        if (result.isEmpty()) {
            System.arraycopy(EMPTY_ACCOUNTS_LIST, 0, responseBuf, 0, EMPTY_ACCOUNTS_LIST.length);
            return EMPTY_ACCOUNTS_LIST.length;
        } else {
            int index = 0;
            System.arraycopy(ACCOUNTS_LIST_START, 0, responseBuf, 0, ACCOUNTS_LIST_START.length);
            index+=ACCOUNTS_LIST_START.length;
            for (int i = 0; i < result.size(); i++) {
                if (i != 0) {
                    responseBuf[index++] = ',';
                }
                index = accountFormatter.formatSuggest(result.get(i), responseBuf, index);
            }
            System.arraycopy(LIST_END, 0, responseBuf, index, LIST_END.length);
            index+=LIST_END.length;
            ObjectPool.releaseSuggestList(result);
            return index;
        }
    }


    public void create(byte[] body, int from, int length) {
        AccountDTO accountDTO = accountParser.parse(body, from, length);
        accountService.add(accountDTO);
    }


    public void update(byte[] body, int from, int length, int id) {
        AccountDTO accountDTO = accountParser.parse(body, from, length);
        accountDTO.id = id;
        accountService.update(accountDTO);
    }


    public void like(byte[] body, int from, int length) {
        List<LikeRequest> requests = likeParser.parse(body, from, length);
        accountService.like(requests);
    }


}
