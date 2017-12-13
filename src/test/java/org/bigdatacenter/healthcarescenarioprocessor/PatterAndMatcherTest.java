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
    public void testCreationPatternAndMatcher() {
        final String query = "CREATE TABLE scenario_tmp.Result1_1511251604834 STORED AS ORC AS SELECT hhidwon,m1,m2,hhid,pid,pidwon,hpid,iflag FROM khpind.khpind_mti_2008 WHERE hhidwon=1";

        // db.table
        final Pattern pattern1 = Pattern.compile("(?<=CREATE\\sTABLE\\s)\\w+\\.\\w+(?=\\sSTORED)");
        final Matcher matcher1 = pattern1.matcher(query);

        if (matcher1.find()) {
            System.err.println(matcher1.group());
            assertThat(matcher1.group(), is("scenario_tmp.Result1_1511251604834"));
        }

        // header
        final Pattern pattern2 = Pattern.compile("(?<=SELECT\\s)[\\w,]+(?=\\sFROM)");
        final Matcher matcher2 = pattern2.matcher(query);

        if (matcher2.find()) {
            System.err.println(matcher2.group());
            assertThat(matcher2.group(), is("hhidwon,m1,m2,hhid,pid,pidwon,hpid,iflag"));
        }


        // db
        final Pattern pattern3 = Pattern.compile("(?<=CREATE\\sTABLE\\s)\\w+(?=\\.)");
        final Matcher matcher3 = pattern3.matcher(query);

        if (matcher3.find()) {
            System.err.println(matcher3.group());
            assertThat(matcher3.group(), is("scenario_tmp"));
        }


        // table
        final Pattern pattern4 = Pattern.compile("(?<=CREATE\\sTABLE\\s\\w{1,128}\\.)\\w+");
        final Matcher matcher4 = pattern4.matcher(query);

        if (matcher4.find()) {
            System.err.println(matcher4.group());
            assertThat(matcher4.group(), is("Result1_1511251604834"));
        }

        // select
        final Pattern pattern5 = Pattern.compile("(?<=STORED\\sAS\\sORC\\sAS\\s)\\w.+");
        final Matcher matcher5 = pattern5.matcher(query);

        if (matcher5.find()) {
            System.err.println(matcher5.group());
            assertThat(matcher5.group(), is("SELECT hhidwon,m1,m2,hhid,pid,pidwon,hpid,iflag FROM khpind.khpind_mti_2008 WHERE hhidwon=1"));
        }
    }

    @Test
    public void testExtractionPatternAndMatcher() {
        final String query = "SELECT hhidwon,m1,m2,hhid,pid,pidwon,hpid,iflag FROM workflow.Result1_1511417255364 WHERE";

        // db.table
        final Pattern pattern1 = Pattern.compile("(?<=FROM\\s)[\\w.]+");
        final Matcher matcher1 = pattern1.matcher(query);

        if (matcher1.find()) {
            System.err.println(matcher1.group());
            assertThat(matcher1.group(), is("workflow.Result1_1511417255364"));
        }

        // header
        final Pattern pattern2 = Pattern.compile("(?<=SELECT\\s)[\\w,]+(?=\\sFROM)");
        final Matcher matcher2 = pattern2.matcher(query);

        if (matcher2.find()) {
            System.err.println(matcher2.group());
            assertThat(matcher2.group(), is("hhidwon,m1,m2,hhid,pid,pidwon,hpid,iflag"));
        }

        // db
        final Pattern pattern3 = Pattern.compile("(?<=FROM\\s)\\w+(?=\\.)");
        final Matcher matcher3 = pattern3.matcher(query);

        if (matcher3.find()) {
            System.err.println(matcher3.group());
            assertThat(matcher3.group(), is("workflow"));
        }

        // table
        final Pattern pattern4 = Pattern.compile("(?<=FROM\\s\\w{1,128}\\.)\\w+");
        final Matcher matcher4 = pattern4.matcher(query);

        if (matcher4.find()) {
            System.err.println(matcher4.group());
            assertThat(matcher4.group(), is("Result1_1511417255364"));
        }
    }

    @Test
    public void testSelection() {
        final String query = "SELECT L.person_id,L.sex,L.age_group,L.sido,L.bp_high,L.bp_lwst,L.ctrb_pt_type_cd,R.trt_org_tp,R.person_id AS person_id_2008 FROM workflow.Result4_1512350236080 L LEFT JOIN workflow.Result7_1512350875214 R ON L.person_id = R.person_id";

        final Pattern headerPattern = Pattern.compile("(?<=SELECT\\s)[\\w,.]+(?=\\AS)");
        final Matcher matcher = headerPattern.matcher(query);

        if (matcher.find()) {
            System.err.println(matcher.group());
            assertThat(matcher.group(), is("Result1_1511417255364"));
        }
    }
}