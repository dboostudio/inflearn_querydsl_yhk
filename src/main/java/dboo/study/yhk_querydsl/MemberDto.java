package dboo.study.yhk_querydsl;

import lombok.Data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MemberDto {

    private String username;
    private int age;

}
