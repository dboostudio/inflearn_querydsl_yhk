package dboo.study.yhk_querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.persistence.EntityManager;
import javax.transaction.Transactional;

import java.util.List;

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

}
