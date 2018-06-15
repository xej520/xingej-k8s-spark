FROM centos7:1.0

ADD spark-2.1.0-bin-hadoop2.7.tgz /usr/local
ADD jdk-8u111-linux-x64.gz /usr/local
ADD setup.sh /usr/local

RUN chmod +x /usr/local/setup.sh

RUN cd /usr/local && ln -s jdk1.8.0_111 java
RUN cd /usr/local && ln -s spark-2.1.0-bin-hadoop2.7 spark

ENV SPARK_HOME=/usr/local/spark \
    JAVA_HOME=/usr/local/java   \
    CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar \
    PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin \
    ROLE="" 			\
    MASTER=""

#ENTRYPOINT ["/usr/local/setup.sh"]
CMD ["/usr/local/setup.sh"]