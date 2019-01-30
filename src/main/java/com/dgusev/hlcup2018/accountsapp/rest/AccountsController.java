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

    public static final byte[] EMPTY_ACCOUNTS_LIST = "{\"accounts\": []}".getBytes();
    private static final byte[] ACCOUNTS_LIST_START = "{\"accounts\": [".getBytes();
    private static final byte[] LIST_END = "]}".getBytes();
    public static final byte[] EMPTY_GROUPS_LIST = "{\"groups\": []}".getBytes();
    private static final byte[] GROUPS_LIST_START = "{\"groups\": [".getBytes();

    @Autowired
    private AccountService accountService;

    @Autowired
    private AccountFormatter accountFormatter;

    @Autowired
    private GroupFormatter groupFormatter;

    @Autowired
    private Dictionary dictionary;

    @Autowired
    private IndexHolder indexHolder;

    private static final ThreadLocal<List<AbstractPredicate>> predicateList = new ThreadLocal<List<AbstractPredicate>>() {
        @Override
        protected List<AbstractPredicate> initialValue() {
            return new ArrayList<>(10);
        }
    };

    private static final ThreadLocal<List<String>> fieldsList = new ThreadLocal<List<String>>() {
        @Override
        protected List<String> initialValue() {
            return new ArrayList<>(10);
        }
    };

    private static final ThreadLocal<AbstractPredicate[]> predicateTemplateArray = new ThreadLocal<AbstractPredicate[]>() {
        @Override
        protected AbstractPredicate[] initialValue() {
            AbstractPredicate[] abstractPredicates = new AbstractPredicate[30];
            abstractPredicates[BirthGtPredicate.ORDER] = new BirthGtPredicate();
            abstractPredicates[BirthLtPredicate.ORDER] = new BirthLtPredicate();
            abstractPredicates[BirthYearPredicate.ORDER] = new BirthYearPredicate();
            abstractPredicates[CityAnyPredicate.ORDER] = new CityAnyPredicate();
            abstractPredicates[CityEqPredicate.ORDER] = new CityEqPredicate();
            abstractPredicates[CityNullPredicate.ORDER] = new CityNullPredicate();
            abstractPredicates[CountryEqPredicate.ORDER] = new CountryEqPredicate();
            abstractPredicates[CountryNullPredicate.ORDER] = new CountryNullPredicate();
            abstractPredicates[EmailDomainPredicate.ORDER] = new EmailDomainPredicate();
            abstractPredicates[EmailEqPredicate.ORDER] = new EmailEqPredicate();
            abstractPredicates[EmailGtPredicate.ORDER] = new EmailGtPredicate();
            abstractPredicates[EmailLtPredicate.ORDER] = new EmailLtPredicate();
            abstractPredicates[FnameAnyPredicate.ORDER] = new FnameAnyPredicate();
            abstractPredicates[FnameEqPredicate.ORDER] = new FnameEqPredicate();
            abstractPredicates[FnameNullPredicate.ORDER] = new FnameNullPredicate();
            abstractPredicates[InterestsAnyPredicate.ORDER] = new InterestsAnyPredicate();
            abstractPredicates[InterestsContainsPredicate.ORDER] = new InterestsContainsPredicate();
            abstractPredicates[JoinedYearPredicate.ORDER] = new JoinedYearPredicate();
            abstractPredicates[LikesContainsPredicate.ORDER] = new LikesContainsPredicate();
            abstractPredicates[PhoneCodePredicate.ORDER] = new PhoneCodePredicate();
            abstractPredicates[PhoneEqPredicate.ORDER] = new PhoneEqPredicate();
            abstractPredicates[PhoneNullPredicate.ORDER] = new PhoneNullPredicate();
            abstractPredicates[PremiumNowPredicate.ORDER] = new PremiumNowPredicate();
            abstractPredicates[PremiumNullPredicate.ORDER] = new PremiumNullPredicate();
            abstractPredicates[SexEqPredicate.ORDER] = new SexEqPredicate();
            abstractPredicates[SnameEqPredicate.ORDER] = new SnameEqPredicate();
            abstractPredicates[SnameNullPredicate.ORDER] = new SnameNullPredicate();
            abstractPredicates[SnameStartsPredicate.ORDER] = new SnameStartsPredicate();
            abstractPredicates[StatusEqPredicate.ORDER] = new StatusEqPredicate();
            abstractPredicates[StatusNEqPredicate.ORDER] = new StatusNEqPredicate();
            return abstractPredicates;
        }
    };


    public int accountsFilter(Map<String, String> allRequestParams, byte[] responseBuf) throws Exception {
        List<AbstractPredicate> predicates = predicateList.get();
        predicates.clear();
        int limit = 0;
        List<String> fields = fieldsList.get();
        fields.clear();
        fields.add("id");
        fields.add("email");
        int predicateMask = 0;
        boolean empty = false;
        AbstractPredicate[] predicateTemplates = predicateTemplateArray.get();
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
                    predicates.add(((SexEqPredicate)predicateTemplates[SexEqPredicate.ORDER]).setValue(ConvertorUtills.convertSex(parameter.getValue())));
                    predicateMask |=2;
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("email_")) {
                if (name.equals("email_domain")) {
                    predicates.add(((EmailDomainPredicate)predicateTemplates[EmailDomainPredicate.ORDER]).setValue(parameter.getValue()));
                } else if (name.equals("email_lt")) {
                    String value = parameter.getValue();
                    if (compareTo(value, indexHolder.minEmail) < 0) {
                        empty = true;
                    }
                    predicates.add(((EmailLtPredicate)predicateTemplates[EmailLtPredicate.ORDER]).setValue(value));
                } else if (name.equals("email_gt")) {
                    predicates.add(((EmailGtPredicate)predicateTemplates[EmailGtPredicate.ORDER]).setValue(parameter.getValue()));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("status_")) {
                if (name.equals("status_eq")) {
                    predicates.add(((StatusEqPredicate)predicateTemplates[StatusEqPredicate.ORDER]).setValue(ConvertorUtills.convertStatusNumber(parameter.getValue())));
                } else if (name.equals("status_neq")) {
                    predicates.add(((StatusNEqPredicate)predicateTemplates[StatusNEqPredicate.ORDER]).setValue(ConvertorUtills.convertStatusNumber(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }

            } else if (name.startsWith("fname_")) {
                if (name.equals("fname_eq")) {
                    predicates.add(((FnameEqPredicate)predicateTemplates[FnameEqPredicate.ORDER]).setValue(dictionary.getFname(parameter.getValue())));
                } else if (name.equals("fname_any")) {
                    String[] fnames = parameter.getValue().split(",");
                    int[] values = new int[fnames.length];
                    for (int i = 0; i < fnames.length; i++) {
                        values[i] = dictionary.getFname(fnames[i]);
                    }

                    predicates.add(((FnameAnyPredicate)predicateTemplates[FnameAnyPredicate.ORDER]).setValue(values));
                    predicateMask |=1;
                } else if (name.equals("fname_null")) {
                    predicates.add(((FnameNullPredicate)predicateTemplates[FnameNullPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("sname_")) {
                if (name.equals("sname_eq")) {
                    predicates.add(((SnameEqPredicate)predicateTemplates[SnameEqPredicate.ORDER]).setValue(dictionary.getFname(parameter.getValue())));
                } else if (name.equals("sname_starts")) {
                    predicates.add(((SnameStartsPredicate)predicateTemplates[SnameStartsPredicate.ORDER]).setValue(parameter.getValue(), dictionary));
                } else if (name.equals("sname_null")) {
                    predicates.add(((SnameNullPredicate)predicateTemplates[SnameNullPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));

                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("phone_")) {
                if (name.equals("phone_code")) {
                    predicates.add(((PhoneCodePredicate)predicateTemplates[PhoneCodePredicate.ORDER]).setValue(parameter.getValue()));
                } else if (name.equals("phone_null")) {
                    predicates.add(((PhoneNullPredicate)predicateTemplates[PhoneNullPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("country_")) {
                if (name.equals("country_eq")) {
                    predicateMask|=1<<2;
                    byte countryIndex = dictionary.getCountry(parameter.getValue());
                    predicates.add(((CountryEqPredicate)predicateTemplates[CountryEqPredicate.ORDER]).setValue(countryIndex));
                } else if (name.equals("country_null")) {
                    predicates.add(((CountryNullPredicate)predicateTemplates[CountryNullPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("city_")) {
                if (name.equals("city_eq")) {
                    predicateMask|=1<<5;
                    predicates.add(((CityEqPredicate)predicateTemplates[CityEqPredicate.ORDER]).setValue(dictionary.getCity(parameter.getValue())));
                } else if (name.equals("city_any")) {
                    String[] cities = parameter.getValue().split(",");
                    int[] values = new int[cities.length];
                    for (int i = 0; i < cities.length; i++) {
                        values[i] = dictionary.getCity(cities[i]);
                    }
                    predicates.add(((CityAnyPredicate)predicateTemplates[CityAnyPredicate.ORDER]).setValue(values));
                } else if (name.equals("city_null")) {
                    predicates.add(((CityNullPredicate)predicateTemplates[CityNullPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("birth_")) {
                if (name.equals("birth_lt")) {
                    predicates.add(((BirthLtPredicate)predicateTemplates[BirthLtPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else if (name.equals("birth_gt")) {
                    predicates.add(((BirthGtPredicate)predicateTemplates[BirthGtPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else if (name.equals("birth_year")) {
                    predicateMask|=1<<3;
                    predicates.add(((BirthYearPredicate)predicateTemplates[BirthYearPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("interests_")) {
                if (name.equals("interests_contains")) {
                    predicateMask|=1<<4;
                    String[] interests = parameter.getValue().split(",");
                    byte[] values = new byte[interests.length];
                    for (int i = 0; i < interests.length; i++) {
                        values[i] = dictionary.getInteres(interests[i]);
                    }
                    predicates.add(((InterestsContainsPredicate)predicateTemplates[InterestsContainsPredicate.ORDER]).setValue(values));
                } else if (name.equals("interests_any")) {
                    String[] interests = parameter.getValue().split(",");
                    byte[] values = new byte[interests.length];
                    for (int i = 0; i < interests.length; i++) {
                        values[i] = dictionary.getInteres(interests[i]);
                    }
                    predicates.add(((InterestsAnyPredicate)predicateTemplates[InterestsAnyPredicate.ORDER]).setValue(values));

                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("likes_")) {
                if (name.equals("likes_contains")) {
                    predicates.add(((LikesContainsPredicate)predicateTemplates[LikesContainsPredicate.ORDER]).setValue(Arrays.stream(parameter.getValue().split(",")).mapToInt(Integer::parseInt).toArray()));
                } else {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.startsWith("premium_")) {
                if (name.equals("premium_now")) {
                    predicates.add(((PremiumNowPredicate)predicateTemplates[PremiumNowPredicate.ORDER]));
                } else if (name.equals("premium_null")) {
                    predicates.add(((PremiumNullPredicate)predicateTemplates[PremiumNullPredicate.ORDER]).setValue(Integer.parseInt(parameter.getValue())));
                } else {
                    throw new BadRequest();
                }
            } else {
                throw BadRequest.INSTANCE;
            }
        }
        if (empty) {
            return 0;
        }
        List<Account> result = accountService.filter(predicates, limit, predicateMask);
        if (result.isEmpty()) {
            return 0;
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
                return 0;
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
                throw NotFoundRequest.INSTANCE;
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
                        throw BadRequest.INSTANCE;
                    }
                } else if (name.equals("country")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw BadRequest.INSTANCE;
                    }
                    country = dictionary.getCountry(parameter.getValue());
                } else if (name.equals("city")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw BadRequest.INSTANCE;
                    }
                    city = dictionary.getCity(parameter.getValue());
                } else {
                    throw BadRequest.INSTANCE;
                }
            }

            List<Account> result = accountService.recommend(id, country, city, limit);
            if (result.isEmpty()) {
                return 0;
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
            throw NotFoundRequest.INSTANCE;
        }
        int limit = 0;
        byte country = -1;
        int city = -1;
        List<Predicate<Account>> predicates = new ArrayList<>();
        for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
            String name = parameter.getKey();
            if (name.equals("query_id")) {
                continue;
            } else if (name.equals("limit")) {
                limit = Integer.parseInt(parameter.getValue());
                if (limit <= 0) {
                    throw BadRequest.INSTANCE;
                }
            } else if (name.equals("country")) {
                if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                    throw BadRequest.INSTANCE;
                }
                country = dictionary.getCountry(parameter.getValue());
            } else if (name.equals("city")) {
                if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                    throw BadRequest.INSTANCE;
                }
                city = dictionary.getCity(parameter.getValue());
            } else {
                throw BadRequest.INSTANCE;
            }
        }

        List<Account> result = accountService.suggest(id, country, city, limit);
        if (result.isEmpty()) {
            return 0;
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


}
