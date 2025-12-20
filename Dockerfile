FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
COPY target/*.jar app.jar
ENV APP_CLASS=""
# We use shell form here to allow variable expansion
ENTRYPOINT java -cp app.jar $APP_CLASS