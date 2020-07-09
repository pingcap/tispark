BASEDIR=$(dirname $0)/..

cd $BASEDIR

mvn clean compile -DskipTests -Pspark-3.0-scala-2.12
mvn package -DskipTests -Pspark-3.0-scala-2.12
