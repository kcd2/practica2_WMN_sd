FROM ubuntu:22.04
RUN apt-get update && apt-get install -y gcc sqlite3 libsqlite3-dev librdkafka-dev
WORKDIR /app
COPY . .
RUN mkdir -p /app/db_data
RUN gcc -o WM_Central WM_Central/WM_Central.c -lsqlite3 -lrdkafka -lpthread
CMD ["./WM_Central", "8080", "broker:29092"]
