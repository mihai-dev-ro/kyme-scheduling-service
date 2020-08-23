from openjdk:8

ARG HOME=/root
ENV HOME=${HOME:-"/root"}

WORKDIR /opt/docker
COPY target/universal/stage /opt/docker

USER root
RUN ["chmod", "-R", "u=rX,g=rX", "/opt/docker"]
RUN ["chmod", "u+x,g+x", "/opt/docker/bin/kyme-scheduling-service"]

# expose 7900/TCP (kyme)
EXPOSE 7900/tcp

CMD [ "/opt/docker/bin/kyme-scheduling-service" ]

