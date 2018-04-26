FROM multiarch/crossbuild

RUN apt-get -y remove cmake       \
  && apt-get -qq update           \
  && apt-get -qq -y install bison

RUN cd /opt/ && \
    BASE_URL_8=http://download.oracle.com/otn-pub/java/jdk/8u171-b11/512cd62ec5174c3487ac17c61aaa89e8/jdk-8u171 && \
    JDK_VERSION=`echo $BASE_URL_8 | rev | cut -d "/" -f1 | rev` && \
    DOWNLOAD_NUMBER=`echo $JDK_VERSION | cut -c 7-` && \
    PLATFORM="-linux-x64.tar.gz" && \
    wget -c -O "${JDK_VERSION}${PLATFORM}" --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" "${BASE_URL_8}${PLATFORM}" && \
    tar xzf "${JDK_VERSION}${PLATFORM}" && \
    rm "${JDK_VERSION}${PLATFORM}"

RUN cd /opt && \
    MVN_VERSION="3.5.3" && \
    wget "http://apache.40b.nl/maven/maven-3/${MVN_VERSION}/binaries/apache-maven-${MVN_VERSION}-bin.tar.gz" && \
    tar xzf "apache-maven-${MVN_VERSION}-bin.tar.gz" && \
    rm "apache-maven-${MVN_VERSION}-bin.tar.gz"

RUN curl https://cmake.org/files/v3.11/cmake-3.11.1-Linux-x86_64.tar.gz --output cmake-3.11.1-Linux-x86_64.tar.gz \
 && tar xzf cmake-3.11.1-Linux-x86_64.tar.gz                                                                      \
 && mv cmake-3.11.1-Linux-x86_64 /opt/                                                                            \
 && rm cmake-3.11.1-Linux-x86_64.tar.gz

ENV JAVA_HOME="/opt/jdk1.8.0_171"
ENV M2_HOME="/opt/maven"
ENV PATH="/opt/cmake-3.11.1-Linux-x86_64/bin:/opt/maven/bin:/opt/jdk1.8.0_171/bin:${PATH}"
