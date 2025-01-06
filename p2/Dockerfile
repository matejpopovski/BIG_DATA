FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y python3-pip

WORKDIR /
COPY inputs /inputs/
COPY partitions /partitions/
COPY wins /wins/
COPY server.py /
COPY client.py /
COPY matchdb.proto /
COPY requirements.txt /

RUN chmod -R +x inputs/ partitions/ wins/
RUN chmod +x server.py client.py matchdb.proto requirements.txt

RUN pip3 install -r requirements.txt --break-system-packages

RUN python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. matchdb.proto

EXPOSE 5440

CMD ["python3", "-u", "/server.py"]
