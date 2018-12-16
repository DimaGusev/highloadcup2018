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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@RestController
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


    @GetMapping(value = "/accounts/filter/", produces = "application/json")
    public ResponseEntity<String> accountsFilter(@RequestParam Map<String,String> allRequestParams) {
        List<Predicate<AccountDTO>> predicates = new ArrayList<>();
        int limit = 0;
        try {
            Set<String> fields = new HashSet<>();
            fields.add("id");
            fields.add("email");
            for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                }
                if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue());
                    continue;
                }
                fields.add(name.substring(0, name.indexOf("_")));
                if (name.startsWith("sex_")) {
                    if (name.equals("sex_eq")) {
                        predicates.add(new SexEqPredicate(parameter.getValue()));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("email_")) {
                    if (name.equals("email_domain")) {
                        predicates.add(new EmailDomainPredicate(parameter.getValue()));
                    } else if (name.equals("email_lt")) {
                        predicates.add(new EmailLtPredicate(parameter.getValue()));
                    } else if (name.equals("email_gt")) {
                        predicates.add(new EmailGtPredicate(parameter.getValue()));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("status_")) {
                    if (name.equals("status_eq")) {
                        predicates.add(new StatusEqPredicate(parameter.getValue()));
                    } else if (name.equals("status_neq")) {
                        predicates.add(new StatusNEqPredicate(parameter.getValue()));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }

                } else if (name.startsWith("fname_")) {
                    if (name.equals("fname_eq")) {
                        predicates.add(new FnameEqPredicate(parameter.getValue()));
                    } else if (name.equals("fname_any")) {
                        predicates.add(new FnameAnyPredicate(Arrays.asList(parameter.getValue().split(","))));
                    } else if (name.equals("fname_null")) {
                        predicates.add(new FnameNullPredicate(Integer.valueOf(parameter.getValue())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("sname_")) {
                    if (name.equals("sname_eq")) {
                        predicates.add(new SnameEqPredicate(parameter.getValue()));
                    } else if (name.equals("sname_starts")) {
                        predicates.add(new SnameStartsPredicate(parameter.getValue()));
                    } else if (name.equals("sname_null")) {
                        predicates.add(new SnameNullPredicate(Integer.valueOf(parameter.getValue())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("phone_")) {
                    if (name.equals("phone_code")) {
                        predicates.add(new PhoneCodePredicate(parameter.getValue()));
                    } else if (name.equals("phone_null")) {
                        predicates.add(new PhoneNullPredicate(Integer.valueOf(parameter.getValue())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("country_")) {
                    if (name.equals("country_eq")) {
                        predicates.add(new CountryEqPredicate(parameter.getValue()));
                    } else if (name.equals("country_null")) {
                        predicates.add(new CountryNullPredicate(Integer.valueOf(parameter.getValue())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("city_")) {
                    if (name.equals("city_eq")) {
                        predicates.add(new CityEqPredicate(parameter.getValue()));
                    } else if (name.equals("city_any")) {
                        predicates.add(new CityAnyPredicate(Arrays.asList(parameter.getValue().split(","))));
                    } else if (name.equals("city_null")) {
                        predicates.add(new CityNullPredicate(Integer.valueOf(parameter.getValue())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("birth_")) {
                    if (name.equals("birth_lt")) {
                        predicates.add(new BirthLtPredicate(Integer.valueOf(parameter.getValue())));
                    } else if (name.equals("birth_gt")) {
                        predicates.add(new BirthGtPredicate(Integer.valueOf(parameter.getValue())));
                    }  else if (name.equals("birth_year")) {
                        predicates.add(new BirthYearPredicate(Integer.valueOf(parameter.getValue())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("interests_")) {
                    if (name.equals("interests_contains")) {
                        predicates.add(new InterestsContainsPredicate(Arrays.asList(parameter.getValue().split(","))));
                    } else if (name.equals("interests_any")) {
                        predicates.add(new InterestsAnyPredicate(Arrays.asList(parameter.getValue().split(","))));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("likes_")) {
                    if (name.equals("likes_contains")) {
                        predicates.add(new LikesContainsPredicate(Arrays.stream(parameter.getValue().split(",")).map(Integer::valueOf).collect(Collectors.toList())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else if (name.startsWith("premium_")) {
                    if (name.equals("premium_now")) {
                        predicates.add(new PremiumNowPredicate(nowProvider.getNow()));
                    } else if (name.equals("premium_null")) {
                        predicates.add(new PremiumNullPredicate(Integer.valueOf(parameter.getValue())));
                    } else {
                        return ResponseEntity.badRequest().build();
                    }
                } else {
                    return ResponseEntity.badRequest().build();
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
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resultTest.toString());
        } catch (NumberFormatException ex) {
            return ResponseEntity.badRequest().build();
        } catch (Exception ex) {
            //ex.printStackTrace();
            return ResponseEntity.badRequest().build();
        }
    }


    @GetMapping(value = "/accounts/group/", produces = "application/json")
    public ResponseEntity<String> group(@RequestParam Map<String,String> allRequestParams) {
        try {
            List<String> keys = new ArrayList<>();
            int order = 1;
            int limit = 0;
            List<Predicate<AccountDTO>> predicates = new ArrayList<>();
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
                    order = Integer.valueOf(parameter.getValue());
                } else if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue());
                } else if (name.equals("sex")) {
                    predicates.add(new SexEqPredicate(parameter.getValue()));
                } else if (name.equals("email")) {
                    predicates.add(new EmailEqPredicate(parameter.getValue()));
                } else if (name.equals("status")) {
                    predicates.add(new StatusEqPredicate(parameter.getValue()));
                } else if (name.equals("fname")) {
                    predicates.add(new FnameEqPredicate(parameter.getValue()));
                } else if (name.equals("sname")) {
                    predicates.add(new SnameEqPredicate(parameter.getValue()));
                } else if (name.equals("phone")) {
                    predicates.add(new PhoneEqPredicate(parameter.getValue()));
                } else if (name.equals("country")) {
                    predicates.add(new CountryEqPredicate(parameter.getValue()));
                } else if (name.equals("city")) {
                    predicates.add(new CityEqPredicate(parameter.getValue()));
                } else if (name.equals("birth")) {
                    predicates.add(new BirthYearPredicate(Integer.valueOf(parameter.getValue())));
                } else if (name.equals("interests")) {
                    predicates.add(new InterestsContainsPredicate(Arrays.asList(parameter.getValue())));
                } else if (name.equals("likes")) {
                    predicates.add(new LikesContainsPredicate(Arrays.asList(Integer.valueOf(parameter.getValue()))));
                } else if (name.equals("joined")) {
                    predicates.add(new JoinedYearPredicate(Integer.valueOf(parameter.getValue())));
                } else {
                    return ResponseEntity.badRequest().build();
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
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resultTest.toString());
        } catch (NumberFormatException | BadRequest ex) {
            return ResponseEntity.badRequest().build();
        } catch (Exception ex) {
            //ex.printStackTrace();
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping(value = "/accounts/{id}/recommend/", produces = "application/json")
    public ResponseEntity<String> recommend(@RequestParam Map<String,String> allRequestParams, @PathVariable("id") Integer id) {
        try {
            int limit = 0;
            List<Predicate<AccountDTO>> predicates = new ArrayList<>();
            for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                } else if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue());
                } else if (name.equals("country")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CountryEqPredicate(parameter.getValue()));
                } else if (name.equals("city")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CityEqPredicate(parameter.getValue()));
                } else {
                    return ResponseEntity.badRequest().build();
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
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resultTest.toString());
        } catch (NumberFormatException | BadRequest ex) {
            return ResponseEntity.badRequest().build();
        } catch (NotFoundRequest notFound) {
            return ResponseEntity.notFound().build();
        } catch (Exception ex) {
            //ex.printStackTrace();
            return ResponseEntity.badRequest().build();
        }

    }


    @GetMapping(value = "/accounts/{id}/suggest/", produces = "application/json")
    public ResponseEntity<String> suggest(@RequestParam Map<String,String> allRequestParams, @PathVariable("id") Integer id) {
        try {
            int limit = 0;
            List<Predicate<AccountDTO>> predicates = new ArrayList<>();
            for (Map.Entry<String, String> parameter : allRequestParams.entrySet()) {
                String name = parameter.getKey();
                if (name.equals("query_id")) {
                    continue;
                } else if (name.equals("limit")) {
                    limit = Integer.valueOf(parameter.getValue());
                } else if (name.equals("country")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CountryEqPredicate(parameter.getValue()));
                } else if (name.equals("city")) {
                    if (parameter.getValue() == null || parameter.getValue().isEmpty()) {
                        throw new BadRequest();
                    }
                    predicates.add(new CityEqPredicate(parameter.getValue()));
                } else {
                    return ResponseEntity.badRequest().build();
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
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resultTest.toString());
        } catch (NumberFormatException | BadRequest ex) {
            return ResponseEntity.badRequest().build();
        } catch (NotFoundRequest notFound) {
            return ResponseEntity.notFound().build();
        } catch (Exception ex) {
            //ex.printStackTrace();
            return ResponseEntity.badRequest().build();
        }

    }


    @PostMapping(value = "/accounts/new/", produces = "application/json")
    public ResponseEntity<String> create(@RequestBody String body, @RequestParam(value = "query_id", required = false) String queryId) {
        if (queryId.equals("1000")) {
            System.out.println(body);
        }
        try {
            AccountDTO accountDTO = accountParser.parse(body.getBytes());
            if (accountDTO.id == 10001) {
                System.out.println("Insert 10001 with query" + queryId);
            }
            accountService.add(accountDTO);
            return ResponseEntity.status(HttpStatus.CREATED).contentType(MediaType.APPLICATION_JSON).body("{}");
        }// catch (NumberFormatException | BadRequest ex) {
            //ex.printStackTrace();
            //System.out.println(body);
            //return ResponseEntity.badRequest().build();
       // }
        catch (Exception ex) {
            if (queryId.equals("1000")) {
                ex.printStackTrace();
            }

           // System.out.println(body);
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping(value = "/accounts/{id}/", produces = "application/json")
    public ResponseEntity<String> update(@RequestBody String body, @PathVariable("id") Integer id) {
        try {
            AccountDTO accountDTO = accountParser.parse(body.getBytes());
            accountDTO.id = id;
            accountService.update(accountDTO);
            return ResponseEntity.status(HttpStatus.ACCEPTED).contentType(MediaType.APPLICATION_JSON).body("{}");
        } catch (NumberFormatException | BadRequest ex) {
            return ResponseEntity.badRequest().build();
        } catch (NotFoundRequest notFound) {
            return ResponseEntity.notFound().build();
        } catch (Exception ex) {
            //ex.printStackTrace();
            return ResponseEntity.badRequest().build();
        }
    }

    @PostMapping(value = "/accounts/likes/", produces = "application/json")
    public ResponseEntity<String> like(@RequestBody String body) {
        try {
            List<LikeRequest> requests = likeParser.parse(body.getBytes());
            accountService.like(requests);
            return ResponseEntity.status(HttpStatus.ACCEPTED).contentType(MediaType.APPLICATION_JSON).body("{}");
        } catch (NumberFormatException | BadRequest ex) {
            return ResponseEntity.badRequest().build();
        } catch (NotFoundRequest notFound) {
            return ResponseEntity.notFound().build();
        } catch (Exception ex) {
            //ex.printStackTrace();
            return ResponseEntity.badRequest().build();
        }
    }


}
