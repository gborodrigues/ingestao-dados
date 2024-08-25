FROM debian:buster-slim

# Instalar dependências de sistema, Python e OpenJDK 11
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openjdk-11-jdk-headless \
    procps \
    && apt-get clean

# Definir o JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Instalar PySpark via pip
RUN pip3 install pyspark

RUN pip3 install duckdb

# Criar alias para garantir que 'python' aponte para 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

# Atualizar pip e instalar dependências do Python
RUN python3 -m pip install --upgrade pip && pip install pyspark

# Setar o diretório de trabalho
WORKDIR /app

# Copiar o conteúdo local para o contêiner
COPY . /app

# Instalar dependências adicionais via requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Comando de execução
CMD ["tail", "-f", "/dev/null"]
#CMD ["python", "your_script.py"] # Substitua 'your_script.py' pelo nome do script que deve ser executado
