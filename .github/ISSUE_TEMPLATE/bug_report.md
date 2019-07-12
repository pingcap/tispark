---
name: Bug report
about: Create a report to help us improve
title: "[BUG] Title of Bug Report"
labels: bug
assignees: ''

---

**Describe the bug**
<!-- A clear and concise description of what the bug is. -->

**What did you do**
<!-- 
If possible, please provide a code receipt to produce this issue.
e.g., 
1. run `spark-shell --jars xxx.jar`
2. in spark-shell, run 
```
val df = spark.sql(“select sum(b) from t where a = 1”)
df.show(20)
```
3. update some data through TiDB
4. in spark-shell, run
```
val df = spark.sql(“select sum(b) from t where a = 1”)
df.show(20)
```
 -->

**What do you expect**
<!-- A clear and concise description of what you expected to happen. -->

**What happens instead**
<!-- If an error occurs, please provide complete error stack. -->

<!-- 
**Screenshots**
If applicable, add screenshots to help explain your problem.
 -->

**Spark and TiSpark version info**
<!-- What version of Spark and TiSpark are you using? (Provide Spark version and run `spark.sql(“select ti_version()”).show(false)` in spark-shell) -->

<!--
**Additional context**
Add any other context about the problem here.
You may also provide TiDB version here if it is related to the issue.
-->
