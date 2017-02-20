export MAVEN_OPTS="" 
##export npm_config_proxy=http://0.0.0.0:8080
mvn   package -DskipTests -Pbuild-distr  -Pspark-2.0 -Phadoop-2.4 -Pyarn -Ppyspark -Psparkr -Pr -Pscala-2.10
