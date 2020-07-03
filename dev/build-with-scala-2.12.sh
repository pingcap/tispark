BASEDIR=$(dirname $0)/..

cd $BASEDIR

mvn clean package -DskipTests -Pspark-3.0-scala-2.12
