FROM python:3.9


RUN apt-get update && apt-get install -y default-jdk


ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin


RUN curl -O https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    && tar xvf spark-3.4.1-bin-hadoop3.tgz \
    && mv spark-3.4.1-bin-hadoop3 /opt/spark \
    && rm spark-3.4.1-bin-hadoop3.tgz


RUN pip install pyspark pyodbc


#COPY mssql-jdbc-12.2.0.jre8.jar $SPARK_HOME/jars/


WORKDIR /bite_DE_task


COPY . /bite_DE_task

VOLUME /bite_DE_task/data
VOLUME /vite_DE_task/output

RUN python -m pip install --upgrade pip virtualenv


RUN python -m virtualenv venv
ENV PATH="/bite_DE_task/venv/Scripts:$PATH"

RUN pip install -r requirements.txt

CMD ["python", "main.py"]