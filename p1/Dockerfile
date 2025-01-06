FROM ubuntu:24.10
RUN apt-get update && apt-get install -y wget unzip
ADD count.sh /
RUN chmod +x /count.sh
CMD ["./count.sh"] 
