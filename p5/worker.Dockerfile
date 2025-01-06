FROM p5-base

ENV SPARK_MASTER_URL=spark://p5-spark-boss-1:7077

CMD ["bash", "-c", "/spark-3.5.3-bin-hadoop3/sbin/start-worker.sh ${SPARK_MASTER_URL} -c 1 -m 1G && tail -f /dev/null"]
