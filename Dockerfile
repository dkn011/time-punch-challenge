FROM postgres
ENV POSTGRES_DB=time_punch
ENV POSTGRES_PASSWORD=time_punch
COPY data_load.sql /docker-entrypoint-initdb.d/
