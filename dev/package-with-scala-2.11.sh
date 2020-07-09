BASEDIR=$(dirname $0)/..

cd $BASEDIR

mvn clean compile -DskipTests -Pspark-2.3-scala-2.11
mvn package -DskipTests -Pspark-2.3-scala-2.11
