FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ABDLab2.Etl.csproj ./
RUN dotnet restore

COPY . ./
RUN dotnet publish -c Release -o /app
RUN mkdir -p /sparkdotnet && find /root/.nuget/packages/microsoft.spark -name "microsoft-spark-*.jar" -exec cp {} /sparkdotnet/microsoft-spark.jar \;

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates openjdk-11-jre-headless tar bash libc6 libgcc-s1 libicu66 libssl1.1 libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz \
       | tar -xz -C /opt \
    && mv /opt/spark-3.1.2-bin-hadoop3.2 /opt/spark

RUN curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh \
    && bash /tmp/dotnet-install.sh --channel 8.0 --runtime dotnet --install-dir /opt/dotnet \
    && rm /tmp/dotnet-install.sh

RUN mkdir -p /opt/microsoft-spark-worker \
    && curl -fsSL https://github.com/dotnet/spark/releases/download/v2.3.1/Microsoft.Spark.Worker.net8.0.linux-x64-2.3.1.tar.gz \
       | tar -xz -C /opt/microsoft-spark-worker --strip-components=1

ENV DOTNET_ROOT=/opt/dotnet
ENV PATH="/opt/dotnet:${PATH}"
ENV DOTNET_WORKER_DIR=/opt/microsoft-spark-worker
ENV SPARK_HOME=/opt/spark
ENV SPARK_LOCAL_IP=127.0.0.1
ENV HOME=/tmp

WORKDIR /app
COPY --from=build /app ./
COPY --from=build /sparkdotnet/microsoft-spark.jar /opt/microsoft-spark/microsoft-spark.jar
COPY submit.sh /app/submit.sh
RUN sed -i '1s/^\xEF\xBB\xBF//' /app/submit.sh \
    && sed -i 's/\r$//' /app/submit.sh \
    && chmod +x /app/submit.sh

ENTRYPOINT ["bash", "/app/submit.sh"]





