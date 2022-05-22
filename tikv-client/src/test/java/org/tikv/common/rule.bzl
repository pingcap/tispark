def junit_suite_test(name, srcs, deps, size="small", resources=[], classpath_resources=[], jvm_flags=[], tags=[], data=[]):
  tests = []
  package = PACKAGE_NAME.replace("src/test/java/", "").replace("/", ".")
  for src in srcs:
    if src.endswith("Test.java"):
      if "/" in src:
         src = package + "." + src.replace("/", ".")
      tests += [src.replace(".java", ".class")]


  native.genrule(
    name = name + "-AllTests-gen",
    outs = ["AllTests.java"],
    cmd = """
      cat <<EOF >> $@
package %s;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({%s})
public class AllTests {}
EOF
    """ % (package, ",".join(tests))
  )

  native.java_test(
    name = name,
    srcs = srcs + ["AllTests.java"],
    test_class = package + ".AllTests",
    resources = resources,
    classpath_resources = classpath_resources,
    data = data,
    size = size,
    tags = tags,
    jvm_flags = jvm_flags,
    deps = deps + [
    ],
  )
