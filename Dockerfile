FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

### change the system source for installing libs
ARG USE_SRC_INSIDE=true
RUN sed -i s/archive.ubuntu.com/mirrors.aliyun.com/g /etc/apt/sources.list ;
RUN sed -i s/security.ubuntu.com/mirrors.aliyun.com/g /etc/apt/sources.list ;
RUN echo "Use aliyun source for installing libs" ;

# 更新包索引
RUN apt-get update
RUN apt-get install -y --no-install-suggests --no-install-recommends \
  vim wget curl dmidecode tzdata poppler-utils python3 python3-pip python3-dev

# setup jupyter
RUN pip install jupyterlab
RUN pip install jupyter-core
RUN pip install --upgrade jupyter
RUN pip install --upgrade ipykernel
RUN pip install ipywidgets
COPY jupyter_lab_config.py /root/.jupyter/jupyter_lab_config.py

# make some useful symlinks that are expected to exist
RUN ln -sf /usr/bin/pydoc3 /usr/local/bin/pydoc
RUN ln -sf /usr/bin/python3 /usr/local/bin/python

# java
RUN apt-get install -y openjdk-8-jdk
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# spark
ARG SOURCE_SPARK
ADD packages/spark/${SOURCE_SPARK} /installs/spark/

WORKDIR /code
ENTRYPOINT ["jupyter-lab", "--port=8888", "--ip=0.0.0.0", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
