# TiSpark Dev Tools Guide

## Formatting

### Scala Format

TiSpark checks [Scalafmt](https://github.com/scalameta/scalafmt/) before building in CI. In order to pass the check, please follow the instructions.

1. In Intellij IDEA, you may import the [scalafmt config file](./.scalafmt.conf) to Intellij following the instructions on [Scalameta](https://scalameta.org/scalafmt/docs/installation.html#intellij). If you want to format each time you save, please check the box of `Reformat on file save`.

2. You may also run [Scala format script](./scalafmt) before you commit & push to corresponding dev branch.

    ```shell script
   ./dev/scalafmt
    ```

### Java Format

TiSpark formats its code using [Google-Java-Format Maven Plugin](https://github.com/coveooss/fmt-maven-plugin) which follows Google's code styleguide. It is also checked on CI before build.

1. In Intellij IDEA

    1. you should download the [Google-Java-format Plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format) via marketplace. Restart IDE, and enable google-java-format by checking the box in `Other Settings`.

    2. you may also use [Java-Google-style xml file](./intellij-java-google-style.xml) and export the schema to Intellij:

        `Preferences`->`Editor`->`Code Style`->`Import Scheme`->`Intellij IDEA Code Style XML`.

2. You may also run [Java format script](./javafmt) before you commit & push to corresponding dev branch.

    ```shell script
   ./dev/javafmt
    ```