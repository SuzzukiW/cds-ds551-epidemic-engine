FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# fixes lack of python in the container
# and that the version of the OS is deprecated
ADD sources.list /etc/apt/sources.list
RUN apt update || : # ignore error
RUN apt install -y python3 python3-pip

# create a directory for the app
RUN mkdir /app
WORKDIR /app
# add the mapper, reducer
ADD mapper.py ./
ADD reducer.py ./
RUN chmod a+x ./*

ENV MAPPER="mapper.py"
ENV REDUCER="reducer.py"
ENV INPUT="/input/README.txt"

ADD run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
