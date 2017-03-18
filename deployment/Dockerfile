FROM java
RUN mkdir -p /usr/src/debs
WORKDIR /usr/src/debs

ENV HOBBIT_SYSTEM_URI=http://project-hobbit.eu/resources/debs2017/ensaisystem
ENV HOBBIT_RABBIT_HOST=rabbit
ENV HOBBIT_SESSION_ID=exp1
ENV SYSTEM_PARAMETERS_MODEL={}
ENV HOBBIT_EXPERIMENT_URI=exp1

ADD ./debs-1.0-SNAPSHOT.jar /usr/src/debs
CMD ["java", "-cp", "debs-1.0-SNAPSHOT.jar", "hobbitadapter.EnsaiAnomalySystemRunner"]
