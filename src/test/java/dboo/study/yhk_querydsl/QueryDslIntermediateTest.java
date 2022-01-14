package dboo.study.yhk_querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import javax.persistence.TypedQuery;
import javax.transaction.Transactional;
import java.util.List;

import static com.querydsl.jpa.JPAExpressions.select;
import static dboo.study.yhk_querydsl.QMember.member;
import static dboo.study.yhk_querydsl.QTeam.team;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
public class QueryDslIntermediateTest {

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

    /**
     * Projection : SELECT 절에서 어떤 컬럼값을 가져오는지 지정하는 것. 수학의 프로젝션과 의미가 비슷하네.
     *
     * 프로젝션 대상이 하나인 경우 반환타입을 명확히 지정 가능
     * 프로젝션 대상이 하나이상인 경우 튜플타입 혹은 DTO로 조회
     */

    @Test
    public void simple_projection() throws Exception {
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void tuple_projection() throws Exception {
        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);

            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    /**
     * JPQL 에서 DTO로 반환받는 법
     */

    @Test
    public void findDto_jpql() throws Exception {
        // jpql에서는 - new operation 활용으로 해야함
        List<MemberDto> result = em.createQuery("select new dboo.study.yhk_querydsl.MemberDto(m.username, m.age) from Member m ", MemberDto.class).getResultList();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * QueryDSL 에서 DTO로 반환 받는 법 3가지 지원
     * - 프로퍼티 접근
     * - 필드 직접 접근
     * - 생성자 사용
     */

    // 프로퍼티 접근 (세터로 값 할당)
    @Test
    public void findDtoBySetter() throws Exception {
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // 프로퍼티 접근 (필드에 값 할당, 세터 무시)
    @Test
    public void findDtoByFields() throws Exception {
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    // 프로퍼티 접근 : DTO의 멤버변수 이름이 다른 경우, subQuery조회결과를 as로 반환할 경우
    @Test
    public void findUserDtoByFields() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class
                        , member.username.as("name")
                        , ExpressionUtils.as(member.username, "name")
                        , ExpressionUtils.as(JPAExpressions.select(memberSub.age.max()).from(memberSub), "age")
                ))
                .from(member)
                .fetch();
        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    // 생성자 접근
    @Test
    public void findDtoByConstructor() throws Exception {
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    public void findUserDtoByConstructor() throws Exception {
        List<UserDto> result = queryFactory
                .select(Projections.constructor(UserDto.class, member.username, member.age))
                .from(member)
                .fetch();
        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }



}
