package com.dgusev.hlcup2018.accountsapp.rest;

import com.dgusev.hlcup2018.accountsapp.format.AccountFormatter;
import com.dgusev.hlcup2018.accountsapp.format.GroupFormatter;
import com.dgusev.hlcup2018.accountsapp.init.NowProvider;
import com.dgusev.hlcup2018.accountsapp.model.*;
import com.dgusev.hlcup2018.accountsapp.parse.AccountParser;
import com.dgusev.hlcup2018.accountsapp.parse.LikeParser;
import com.dgusev.hlcup2018.accountsapp.predicate.*;
import com.dgusev.hlcup2018.accountsapp.service.AccountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Component
public class AccountsController {

    private static final Set<String> ALLOWED_KEYS = new HashSet<>(Arrays.asList("sex", "status", "interests", "country", "city"));

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


    public String accountsFilter(Map<String,List<String>> allRequestParams) throws Exception {
        List<Predicate<AccountDTO>> predicates = new ArrayList<>();
        int limit = 0;
            Set<String> fields = new HashSet<>();
            fields.add("id");
            fields.add("email");
            for (Map.Entry<String, List<String>> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                }
                if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue().get(0));
                    continue;
                }
                fields.add(name.substring(0, name.indexOf("_")));
                if (name.startsWith("sex_")) {
                    if (name.equals("sex_eq")) {
                        predicates.add(new SexEqPredicate(parameter.getValue().get(0)));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("email_")) {
                    if (name.equals("email_domain")) {
                        predicates.add(new EmailDomainPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("email_lt")) {
                        predicates.add(new EmailLtPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("email_gt")) {
                        predicates.add(new EmailGtPredicate(parameter.getValue().get(0)));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("status_")) {
                    if (name.equals("status_eq")) {
                        predicates.add(new StatusEqPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("status_neq")) {
                        predicates.add(new StatusNEqPredicate(parameter.getValue().get(0)));
                    } else {
                        throw new BadRequest();
                    }

                } else if (name.startsWith("fname_")) {
                    if (name.equals("fname_eq")) {
                        predicates.add(new FnameEqPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("fname_any")) {
                        predicates.add(new FnameAnyPredicate(Arrays.asList(parameter.getValue().get(0).split(","))));
                    } else if (name.equals("fname_null")) {
                        predicates.add(new FnameNullPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("sname_")) {
                    if (name.equals("sname_eq")) {
                        predicates.add(new SnameEqPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("sname_starts")) {
                        predicates.add(new SnameStartsPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("sname_null")) {
                        predicates.add(new SnameNullPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("phone_")) {
                    if (name.equals("phone_code")) {
                        predicates.add(new PhoneCodePredicate(parameter.getValue().get(0)));
                    } else if (name.equals("phone_null")) {
                        predicates.add(new PhoneNullPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("country_")) {
                    if (name.equals("country_eq")) {
                        predicates.add(new CountryEqPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("country_null")) {
                        predicates.add(new CountryNullPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("city_")) {
                    if (name.equals("city_eq")) {
                        predicates.add(new CityEqPredicate(parameter.getValue().get(0)));
                    } else if (name.equals("city_any")) {
                        predicates.add(new CityAnyPredicate(Arrays.asList(parameter.getValue().get(0).split(","))));
                    } else if (name.equals("city_null")) {
                        predicates.add(new CityNullPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("birth_")) {
                    if (name.equals("birth_lt")) {
                        predicates.add(new BirthLtPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else if (name.equals("birth_gt")) {
                        predicates.add(new BirthGtPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    }  else if (name.equals("birth_year")) {
                        predicates.add(new BirthYearPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("interests_")) {
                    if (name.equals("interests_contains")) {
                        predicates.add(new InterestsContainsPredicate(Arrays.asList(parameter.getValue().get(0).split(","))));
                    } else if (name.equals("interests_any")) {
                        predicates.add(new InterestsAnyPredicate(Arrays.asList(parameter.getValue().get(0).split(","))));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("likes_")) {
                    if (name.equals("likes_contains")) {
                        predicates.add(new LikesContainsPredicate(Arrays.stream(parameter.getValue().get(0).split(",")).map(Integer::valueOf).collect(Collectors.toList())));
                    } else {
                        throw new BadRequest();
                    }
                } else if (name.startsWith("premium_")) {
                    if (name.equals("premium_now")) {
                        predicates.add(new PremiumNowPredicate(nowProvider.getNow()));
                    } else if (name.equals("premium_null")) {
                        predicates.add(new PremiumNullPredicate(Integer.valueOf(parameter.getValue().get(0))));
                    } else {
                        throw new BadRequest();
                    }
                } else {
                    throw new BadRequest();
                }
            }
            List<AccountDTO> result = accountService.filter(predicates, limit);
            StringBuilder resultTest = new StringBuilder("{\"accounts\": [");
            for (int i = 0; i < result.size(); i++) {
                if (i != 0) {
                    resultTest.append(",");
                }
                resultTest.append(accountFormatter.format(result.get(i), fields));
            }
            resultTest.append("]}");
            return resultTest.toString();
    }

    public String group(Map<String,List<String>> allRequestParams) {
            List<String> keys = new ArrayList<>();
            int order = 1;
            int limit = 0;
            List<Predicate<AccountDTO>> predicates = new ArrayList<>();
            for (Map.Entry<String, List<String>> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                }

                if (name.equals("keys")) {
                    keys.addAll(Arrays.asList(parameter.getValue().get(0).split(",")));

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
                    order = Integer.valueOf(parameter.getValue().get(0));
                } else if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue().get(0));
                } else if (name.equals("sex")) {
                    predicates.add(new SexEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("email")) {
                    predicates.add(new EmailEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("status")) {
                    predicates.add(new StatusEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("fname")) {
                    predicates.add(new FnameEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("sname")) {
                    predicates.add(new SnameEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("phone")) {
                    predicates.add(new PhoneEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("country")) {
                    predicates.add(new CountryEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("city")) {
                    predicates.add(new CityEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("birth")) {
                    predicates.add(new BirthYearPredicate(Integer.valueOf(parameter.getValue().get(0))));
                } else if (name.equals("interests")) {
                    predicates.add(new InterestsContainsPredicate(Arrays.asList(parameter.getValue().get(0))));
                } else if (name.equals("likes")) {
                    predicates.add(new LikesContainsPredicate(Arrays.asList(Integer.valueOf(parameter.getValue().get(0)))));
                } else if (name.equals("joined")) {
                    predicates.add(new JoinedYearPredicate(Integer.valueOf(parameter.getValue().get(0))));
                } else {
                    throw new BadRequest();
                }
            }

            List<Group> groups = accountService.group(keys, predicates, order, limit);
            StringBuilder resultTest = new StringBuilder("{\"groups\": [");
            for (int i = 0; i < groups.size(); i++) {
                if (i != 0) {
                    resultTest.append(",");
                }
                resultTest.append(groupFormatter.format(groups.get(i), keys));
            }
            resultTest.append("]}");
            return resultTest.toString();
    }

    public String recommend(Map<String,List<String>> allRequestParams, Integer id) {
            int limit = 0;
            List<Predicate<AccountDTO>> predicates = new ArrayList<>();
            for (Map.Entry<String, List<String>> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                } else if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue().get(0));
                    if (limit < 0) {
                        throw new BadRequest();
                    }
                } else if (name.equals("country")) {
                    if (parameter.getValue().get(0) == null || parameter.getValue().get(0).isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CountryEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("city")) {
                    if (parameter.getValue().get(0) == null || parameter.getValue().get(0).isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CityEqPredicate(parameter.getValue().get(0)));
                } else {
                    throw new BadRequest();
                }
            }

            List<AccountDTO> result = accountService.recommend(id, predicates, limit);
            StringBuilder resultTest = new StringBuilder("{\"accounts\": [");
            for (int i = 0; i < result.size(); i++) {
                if (i != 0) {
                    resultTest.append(",");
                }
                resultTest.append(accountFormatter.formatRecommend(result.get(i)));
            }
            resultTest.append("]}");
            return resultTest.toString();
    }


    public String suggest(Map<String,List<String>> allRequestParams, Integer id) {
            int limit = 0;
            List<Predicate<AccountDTO>> predicates = new ArrayList<>();
            for (Map.Entry<String, List<String>> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                } else if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue().get(0));
                    if (limit < 0) {
                        throw new BadRequest();
                    }
                } else if (name.equals("country")) {
                    if (parameter.getValue().get(0) == null || parameter.getValue().get(0).isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CountryEqPredicate(parameter.getValue().get(0)));
                } else if (name.equals("city")) {
                    if (parameter.getValue().get(0) == null || parameter.getValue().get(0).isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CityEqPredicate(parameter.getValue().get(0)));
                } else {
                    throw new BadRequest();
                }
            }

            List<AccountDTO> result = accountService.suggest(id, predicates, limit);
            StringBuilder resultTest = new StringBuilder("{\"accounts\": [");
            for (int i = 0; i < result.size(); i++) {
                if (i != 0) {
                    resultTest.append(",");
                }
                resultTest.append(accountFormatter.formatSuggest(result.get(i)));
            }
            resultTest.append("]}");
            return resultTest.toString();
    }



    public void create(String body) {
            AccountDTO accountDTO = accountParser.parse(body.getBytes());
            accountService.add(accountDTO);
    }

    public void update(String body, Integer id) {
            AccountDTO accountDTO = accountParser.parse(body.getBytes());
            accountDTO.id = id;
            accountService.update(accountDTO);
    }


    public void like(String body) {
            List<LikeRequest> requests = likeParser.parse(body.getBytes());
            accountService.like(requests);
    }


}
