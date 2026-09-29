FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Copy the fat jar built by: cd applications/java && mvn package -q
COPY applications/java/target/kafka-monitoring-1.0-SNAPSHOT.jar app.jar

# Optionally mount the New Relic agent jar at runtime:
#   -v ./applications/java/newrelic/newrelic.jar:/app/newrelic.jar:ro
#   -v ./applications/java/newrelic/newrelic.yml:/app/newrelic.yml:ro
# and set JAVA_OPTS=-javaagent:/app/newrelic.jar -Dnewrelic.config.file=/app/newrelic.yml

# Run as non-root
RUN useradd -m -u 1001 appuser
USER 1001

# MAIN_CLASS is set by the compose service
ENV MAIN_CLASS="com.example.kafka.Producer"
ENV JAVA_OPTS=""

CMD sh -c "java ${JAVA_OPTS} -cp app.jar ${MAIN_CLASS}"
