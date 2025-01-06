FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y python3-pip
WORKDIR /
COPY bigdata.py /
COPY csvsum.py /
COPY parquetsum.py /
COPY upload.py /
COPY simple.csv /
COPY table.proto /
COPY server.py /
COPY requirements.txt /

RUN chmod +x bigdata.py csvsum.py parquetsum.py upload.py simple.csv
RUN chmod +x server.py table.proto requirements.txt

RUN pip3 install -r requirements.txt --break-system-packages
RUN python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. table.proto

EXPOSE 5440
CMD ["python3", "/server.py"]
