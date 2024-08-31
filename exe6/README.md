#### Pré Requs

AWS-CLI
Pre req - Já ter feito o setup do aws-configure
Configurar credencias ~/.aws/credentials
Ter criar o Bucket S3 com os dados a serem processados


Conteúdo dos diretórios

#### (raw/delivery/trusted)
|.
|_.env
|_requirements.txt
|_script.py


#### Instalação de libs dependentes
Instalar as libs localmente, as libs são:

No diretório onde se encontra a função (raw/delivery/trusted),criar o arquivo requirements.txt com o seguinte conteúdo:
boto3
pandas==2.2.2

Exemplo:
pip3 install --target ./raw_function -r raw_function/requirements.txt


#### Instalação de libs dependentes
Rodar o script 
python3 <nome do script>.py

#### Parametrizar o template yaml no cloud formation
raw_layer_template.yaml

```
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
  RawLayer:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: raw
      Handler: script.handler # sendo script = <nome do arquivo python> e handle = <nome da função da lambda>
      Runtime: python3.12
      CodeUri: raw_function/ #alterar para a pasta da nova função - Previamente definida pelo passo 2 (raw/delivery/trusted)
      MemorySize: 128
      Role: arn:aws:iam::125892454700:role/LabRole
      Timeout: 15
      Environment: 
        Variables:
          BUCKET_NAME: dados-exe6 #nome do seu bucket
      Events:
        MyScheduledEvent:
          Type: Schedule
          Properties:
            Schedule: cron(0 12 * * ? *)
```


#### (raw/delivery/trusted)
|.
|_raw_function
  |_.env
  |_requirements.txt
  |_script.py

Do diretório raiz da função, executar o zip da função
zip -r9 raw_function.zip raw_function


#### Criar buckets s3 para armazenar os código

Para os scripts que serão carregadso para a lambda
Ex: code-exe6
aws cloudformation package \
    --template-file raw_layer_template.yaml \
    --s3-bucket <seu bucket S3 - Lembrando que os nomes de S3 são únicos para toda a AWS> \ 
    --output-template-file packaged-template.yaml

#### Deploy 
aws cloudformation deploy \
    --template-file packaged-template.yaml \
    --stack-name <nome da sua stack - escolha como preferir> \ 
    --capabilities CAPABILITY_IAM


#### Debug
Acompanhar o serviço pela aplicação cloud formation 