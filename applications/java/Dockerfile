FROM registry.redhat.io/ubi9/openjdk-17-runtime:latest

WORKDIR /app

# Copy the fat jar built by: cd applications/java && mvn package -q
COPY applications/java/target/kafka-monitoring-1.0-SNAPSHOT.jar app.jar

# Optionally mount the New Relic agent jar at runtime:
#   -v ./applications/java/newrelic/newrelic.jar:/app/newrelic.jar:ro
#   -v ./applications/java/newrelic/newrelic.yml:/app/newrelic.yml:ro
# and set JAVA_OPTS=-javaagent:/app/newrelic.jar -Dnewrelic.config.file=/app/newrelic.yml

# Non-root user provided by the base image
USER 185

# MAIN_CLASS is set by the compose service
ENV MAIN_CLASS="com.example.kafka.Producer"
ENV JAVA_OPTS=""

CMD sh -c "java ${JAVA_OPTS} -cp app.jar ${MAIN_CLASS}"
