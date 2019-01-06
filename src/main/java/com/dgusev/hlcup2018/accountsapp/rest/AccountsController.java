package com.dgusev.hlcup2018.accountsapp.rest;

import com.dgusev.hlcup2018.accountsapp.format.AccountFormatter;
import com.dgusev.hlcup2018.accountsapp.format.GroupFormatter;
import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.*;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import com.dgusev.hlcup2018.accountsapp.parse.LikeParser;
import com.dgusev.hlcup2018.accountsapp.pool.ObjectPool;
import com.dgusev.hlcup2018.accountsapp.predicate.*;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import com.dgusev.hlcup2018.accountsapp.service.ConvertorUtills;
import com.dgusev.hlcup2018.accountsapp.service.Dictionary;
import io.netty.buffer.ByteBuf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Predicate;

@Component
public class AccountsController {

    private static final Set<String> ALLOWED_KEYS = new HashSet<>(Arrays.asList("sex", "status", "interests", "country", "city"));

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
    private NowProvider nowProvider;

    @Autowired
    private LikeParser likeParser;

    @Autowired
    private Dictionary dictionary;

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


    public void accountsFilter(Map<String, String> allRequestParams, ByteBuf responseBuf) throws Exception {
        List<Predicate<Account>> predicates = predicateList.get();
        predicates.clear();
        int limit = 0;
        List<String> fields = fieldsList.get();
        fields.clear();
        fields.add("id");
        fields.add("email");
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
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("email_")) {
                if (name.equals("email_domain")) {
                    predicates.add(new EmailDomainPredicate(parameter.getValue()));
                } else if (name.equals("email_lt")) {
                    predicates.add(new EmailLtPredicate(parameter.getValue()));
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
                    predicates.add(new PremiumNowPredicate(nowProvider.getNow()));
                } else if (name.equals("premium_null")) {
                    predicates.add(new PremiumNullPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw new BadRequest();
                }
            } else {
                throw BadRequest.INSTANCE;
            }
        }
        List<Account> result = accountService.filter(predicates, limit);
        if (result.isEmpty()) {
            responseBuf.writeBytes(EMPTY_ACCOUNTS_LIST);
        } else {
            byte[] arr = ObjectPool.acquireFormatterArray();
            responseBuf.writeBytes(ACCOUNTS_LIST_START);
            for (int i = 0; i < result.size(); i++) {
                if (i != 0) {
                    responseBuf.writeByte(',');
                }
                accountFormatter.format(result.get(i), fields, responseBuf, arr);
            }
            responseBuf.writeBytes(LIST_END);
        }
    }

    public void group(Map<String,String> allRequestParams, ByteBuf responseBuf) {
            List<String> keys = new ArrayList<>();
            int order = 1;
            int limit = 0;
            List<Predicate<Account>> predicates = new ArrayList<>();
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
                        if (!ALLOWED_KEYS.contains(key)) {
                            throw new BadRequest();
                        }
                    }

                } else if (name.equals("order")) {
                    order = Integer.parseInt(parameter.getValue());
                } else if (name.equals("limit")) {
                    limit = Integer.parseInt(parameter.getValue());
                } else if (name.equals("sex")) {
                    predicates.add(new SexEqPredicate(ConvertorUtills.convertSex(parameter.getValue())));
                } else if (name.equals("email")) {
                    predicates.add(new EmailEqPredicate(parameter.getValue()));
                } else if (name.equals("status")) {
                    predicates.add(new StatusEqPredicate(ConvertorUtills.convertStatusNumber(parameter.getValue())));
                } else if (name.equals("fname")) {
                    predicates.add(new FnameEqPredicate(dictionary.getFname(parameter.getValue())));
                } else if (name.equals("sname")) {
                    predicates.add(new SnameEqPredicate(dictionary.getSname(parameter.getValue())));
                } else if (name.equals("phone")) {
                    predicates.add(new PhoneEqPredicate(parameter.getValue()));
                } else if (name.equals("country")) {
                    predicates.add(new CountryEqPredicate(dictionary.getCountry(parameter.getValue())));
                } else if (name.equals("city")) {
                    predicates.add(new CityEqPredicate(dictionary.getCity(parameter.getValue())));
                } else if (name.equals("birth")) {
                    predicates.add(new BirthYearPredicate(Integer.parseInt(parameter.getValue())));
                } else if (name.equals("interests")) {
                    byte[] value = new byte[1];
                    value[0] = dictionary.getInteres(parameter.getValue());
                    predicates.add(new InterestsContainsPredicate(value));
                } else if (name.equals("likes")) {
                    int[] array = new int[1];
                    array[0] = Integer.parseInt(parameter.getValue());
                    predicates.add(new LikesContainsPredicate(array));
                } else if (name.equals("joined")) {
                    predicates.add(new JoinedYearPredicate(Integer.parseInt(parameter.getValue())));
                } else {
                    throw new BadRequest();
                }
            }

            List<Group> groups = accountService.group(keys, predicates, order, limit);
            if (groups.isEmpty()) {
                responseBuf.writeBytes(EMPTY_GROUPS_LIST);
            } else {
                responseBuf.writeBytes(GROUPS_LIST_START);
                for (int i = 0; i < groups.size(); i++) {
                    if (i != 0) {
                        responseBuf.writeByte(',');
                    }
                    groupFormatter.format(groups.get(i), keys, responseBuf);
                }
                responseBuf.writeBytes(LIST_END);
            }
    }

    public void recommend(Map<String, String> allRequestParams, int id, ByteBuf responseBuf) {
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
                    if (limit < 0) {
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

            List<Account> result = accountService.recommend(id, predicates, limit);
            if (result.isEmpty()) {
                responseBuf.writeBytes(EMPTY_ACCOUNTS_LIST);
            } else {
                responseBuf.writeBytes(ACCOUNTS_LIST_START);
                for (int i = 0; i < result.size(); i++) {
                    if (i != 0) {
                        responseBuf.writeByte(',');
                    }
                    accountFormatter.formatRecommend(result.get(i), responseBuf);
                }
                responseBuf.writeBytes(LIST_END);
            }
    }


    public void suggest(Map<String,String> allRequestParams, int id, ByteBuf responseBuf) {
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
            responseBuf.writeBytes(EMPTY_ACCOUNTS_LIST);
        } else {
            responseBuf.writeBytes(ACCOUNTS_LIST_START);
            byte[] arr = ObjectPool.acquireFormatterArray();
            for (int i = 0; i < result.size(); i++) {
                if (i != 0) {
                    responseBuf.writeByte(',');
                }
                accountFormatter.formatSuggest(result.get(i), responseBuf, arr);
            }
            responseBuf.writeBytes(LIST_END);
        }
        if (result != Collections.EMPTY_LIST ) {
            ObjectPool.releaseSuggestList(result);
        }
    }



    public void create(byte[] body, int length) {
            AccountDTO accountDTO = accountParser.parse(body, length);
            accountService.add(accountDTO);
    }

    public void create(byte[] body, int from, int length) {
        AccountDTO accountDTO = accountParser.parse(body, from, length);
        accountService.add(accountDTO);
    }

    public void update(byte[] body, int length, int id) {
            AccountDTO accountDTO = accountParser.parse(body, length);
            accountDTO.id = id;
            accountService.update(accountDTO);
    }

    public void update(byte[] body, int from, int length, int id) {
        AccountDTO accountDTO = accountParser.parse(body, from, length);
        accountDTO.id = id;
        accountService.update(accountDTO);
    }


    public void like(byte[] body, int length) {
        List<LikeRequest> requests = likeParser.parse(body, length);
        accountService.like(requests);
    }

    public void like(byte[] body, int from, int length) {
        List<LikeRequest> requests = likeParser.parse(body, from, length);
        accountService.like(requests);
    }


}
