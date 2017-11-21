package org.bigdatacenter.healthcarescenarioprocessor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@RunWith(SpringRunner.class)
@SpringBootTest
public class PatterAndMatcherTest {
    @Test
    public void testPatternAndMatcher() {
        final String query = "CREATE TABLE scenario_tmp.Result1_1511251604834 STORED AS ORC AS SELECT hhidwon,m1,m2,hhid,pid,pidwon,hpid,iflag FROM khpind.khpind_mti_2008 WHERE hhidwon=1";

        // db.table
        final Pattern pattern1 = Pattern.compile("(?<=TABLE[ ])\\w+[.]\\w+(?=[ ]STORED)");
        final Matcher matcher1 = pattern1.matcher(query);

        if (matcher1.find())
            assertThat(matcher1.group(), is("scenario_tmp.Result1_1511251604834"));

        // header
        final Pattern pattern2 = Pattern.compile("(?<=SELECT[ ])[\\w,]+(?=[ ]FROM)");
        final Matcher matcher2 = pattern2.matcher(query);

        if (matcher2.find())
            assertThat(matcher2.group(), is("hhidwon,m1,m2,hhid,pid,pidwon,hpid,iflag"));
    }
}
