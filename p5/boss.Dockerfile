FROM p5-base

ENV SPARK_MASTER_HOST=boss

CMD ["bash", "-c", "/spark-3.5.3-bin-hadoop3/sbin/start-master.sh -h boss && tail -f /dev/null"]

