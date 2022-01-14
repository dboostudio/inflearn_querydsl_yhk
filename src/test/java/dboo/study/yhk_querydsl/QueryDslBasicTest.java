package dboo.study.yhk_querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import javax.transaction.Transactional;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static dboo.study.yhk_querydsl.QMember.member;
import static dboo.study.yhk_querydsl.QTeam.team;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@Transactional
public class QueryDslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before(){
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);
        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }


    @Test
    public void startJPQL() {
        //member1을 찾아라.
        String qlString =
                "select m from Member m where m.username = :username";
        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        //member1을 찾아라.
//        QMember m = new QMember("m");
        Member findMember = queryFactory
                .select(member) // QMember.member 대신에 static import를 통해 member로 작성
                .from(member)
                .where(member.username.eq("member1"))//파라미터 바인딩 처리
                .fetchOne();
        Member findMember2 = queryFactory
                .select(member).from(member).where(member.username.eq("member2")).fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember2.getUsername()).isEqualTo("member2");
    }

    @Test
    public void search() {
        Member findMember = queryFactory.selectFrom(member).where(member.username.eq("member1").and(member.age.eq(10))).fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParma() {
        Member findMember = queryFactory.selectFrom(member)
                .where(
                        member.username.eq("member1")
                        , member.age.eq(10) //AND
                ).fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }
    @Test
    public void resultFetch(){
        List<Member> fetch = queryFactory.selectFrom(member).fetch();
        Member fetchOne = queryFactory.selectFrom(QMember.member).fetchOne();
        Member fetchFirst = queryFactory.selectFrom(QMember.member).fetchFirst();
        QueryResults<Member> results = queryFactory.selectFrom(member).fetchResults();
        results.getTotal();
        List<Member> content = results.getResults();

        long count = queryFactory.selectFrom(member).fetchCount();
    }

    /**
     * 정렬 기준
     * 1. 회원 나이 내림차순 (DESC)
     * 2. 회원 이름 올림차순 (ASC)
     * 3. 2에서 회원 이름 Null일 시  마지막 (nulls last)
     */
    @Test
    public void sort(){

        em.persist(new Member( null, 100));
        em.persist(new Member( "member5", 100));
        em.persist(new Member( "member6", 100));

        List<Member> result = queryFactory.selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast()).fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();

    }

    @Test
    public void paging1(){
        List<Member> fetch = queryFactory.selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1).limit(2).fetch();
        assertThat(fetch.size()).isEqualTo(2);
    }

    @Test
    public void paging2(){
        QueryResults<Member> results = queryFactory.selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1).limit(2).fetchResults();

        assertThat(results.getTotal()).isEqualTo(4);
        assertThat(results.getLimit()).isEqualTo(2);
        assertThat(results.getOffset()).isEqualTo(1);
        assertThat(results.getResults().size()).isEqualTo(2);

    }

    @Test
    public void aggregation() {
        // 실무에서는 Tuple보단 DTO(? DAO가 맞는 말 아닌가...?) 로 바로 뽑아오는 방법을 주로 쓴다.
        List<Tuple> result = queryFactory.select(
                member.count(),
                member.age.sum(),
                member.age.avg(),
                member.age.min()
        ).from(member).fetch();


        Tuple tuple = result.get(0); //fetchOne으로 받고, 단일 Tuple로 받으면 해당 라인을 쓸 필요가 없겠지?
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

    }

    @Test
    public void group() throws Exception {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name).fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * teamA에 소속된 모든 회원을 찾기
     */
    @Test
    public void join() throws Exception {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                //.on()으로 조인 제약도 걸 수 있다.
                .where(team.name.eq("teamA"))
                .fetch();
        assertThat(result).extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 회원의 이름이 팀 이름과 같은 회원 조회
     * 연관관계없는 조인의 예(세타조인)
     */
    @Test
    public void theta_join() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result).extracting("username")
                .containsExactly("teamA", "teamB");
    }

    // 예) 회원 팀 조인, 팀 이름이 teamA인것만 조인, 회원 모두 조회
    // JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'

    @Test
    public void join_on_filter() throws Exception {
        List<Tuple> teamA = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA")) // 필터링, innerJoin (join)의 경우에는 의미가 없고, where절에서 처리하는것을 권장.
                .fetch();
        for (Tuple tuple : teamA) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 연관관계가 없는 엔티티를 외부 조인
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     */
    @Test
    public void join_on_no_rel() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
//                .leftJoin(member.team, team)
// 위와 같이 member.team, team으로 조인할 경우 id를 기반으로 조인된다.
// 아래와 같이 team에만 조인을 걸고 제약조건으로 member.username = team.name을 필터링을 준다.
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("Tuple : " + tuple);
        }
    }

    // - Fetch Join
    // SQL이 제공하는 기능은 아니다. join을 사용해서 엔티티를 한번에 조회할때 사용하고, 주로 성능최적화를 위해 사용한다.

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() throws Exception { // fetch join 없을 때
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        System.out.println(findMember);
        // member엔티티에서 team의 페치전략이 lazy이기 때문에 위 쿼리로만은 team은 조회하지 않는다.

        // EntityMangerFactory를 통해 해당 엔티티가 초기화되었는지 확인할 수 있다.
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isEqualTo(false);
    }

    @Test
    public void fetchJoinYes() throws Exception { // fetch join 있을 때
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin() // 페치조인
                .where(member.username.eq("member1"))
                .fetchOne();

        System.out.println(findMember);
        // member엔티티에서 team의 페치전략이 lazy이기 때문에 위 쿼리로만은 team은 조회하지 않는다.

        // EntityMangerFactory를 통해 해당 엔티티가 초기화되었는지 확인할 수 있다.
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isEqualTo(true);
    }

    /***
     * JPA의 subQuery의 한계 : from절에는 subQuery(인라인뷰)를 사용할 수 없다.
     *
     * 해결방안
     *  1. join 을 사용한다.
     *  2. query를 2번 분리하여 사용한다.
     *  3. nativeSQL을 사용한다.
     */

    /**
     * 나이가 가장 많은 회원을 조회
     */
    @Test
    public void subQuery() throws Exception {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        // subQuery에서 alias가 겹치면 안되기때문에, Q타입을 하나 더 다른 alias로 생성하여 사용.
                        select(memberSub.age.max())
                                .from(memberSub)
                )).fetch();

        assertThat(result).extracting("age").containsExactly(40);
    }

    /**
     * 나이가 평균 이상인 회원을 조회
     */
    @Test
    public void subQuery_goe() throws Exception {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        // subQuery에서 alias가 겹치면 안되기때문에, Q타입을 하나 더 다른 alias로 생성하여 사용.
                        select(memberSub.age.avg())
                                .from(memberSub)
                )).fetch();

        assertThat(result).extracting("age").containsExactly(30, 40);
    }

    /**
     * 나이가 평균 이상인 회원을 조회
     */
    @Test
    public void subQuery_in() throws Exception {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        // subQuery에서 alias가 겹치면 안되기때문에, Q타입을 하나 더 다른 alias로 생성하여 사용.
                        // JPAExpressions.select 를 아래와 같이 static import 가능.
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                )).fetch();

        assertThat(result).extracting("age").containsExactly(20, 30, 40);
    }

    /**
     * 나이가 평균 이상인 회원을 조회
     */
    @Test
    public void selectSubQuery() throws Exception {

        QMember memberSub = new QMember("memberSub");


        List<Tuple> result = queryFactory.select(
                member.username,
                select(memberSub.age.avg())
                        .from(memberSub)
        ).from(member).fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
    }

    /***
     * 가급적이면 이런 기능은 어플리케이션단에서 처리하는게 좋다.
     */

    @Test
    public void basic_case() throws Exception {
        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타")
                ).from(member).fetch();
        for (String s : result) {
            System.out.println(s);
        }
    }

    @Test
    public void complex_case() throws Exception {
        List<String> result = queryFactory.select(new CaseBuilder()
                .when(member.age.between(0, 20)).then("0~20")
                .when(member.age.between(21, 30)).then("21~30")
                .otherwise("기타")
        ).from(member).fetch();
        for (String s : result) {
            System.out.println(s);
        }
    }





}
