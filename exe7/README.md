# Como rodar o projeto

### Criando toda a infra

*Primeiramente certifique-se que as chaves de acesso da aws junto ao token estejam configurados.*

Na raiz da pasta temos o arquivo **bootstrap.sh**, primeiramente de permissão de execução

```
chmod +x bootstrap.sh
```

Rode o comando **./bootstrap.sh**, sera pedido o nome do bucket para guardar os dados, o nome da fila e o nome do bucket s3 que servira como repositório de dados da lambdas.
**Importante**: os dados que estão no caminho data_sources/ irão subir também no bucket raw de dados.

### Rodando o producer localmente

Rode o comando de instalação das libs

```
pip3 install --target ./producer -r producer/requirements.txt
```

*Em breve como subir no serviço lambda...*

### Deletando infra

Na raiz da pasta temos o arquivo **delete_cloudformation.sh**, primeiramente de permissão de execução

```
chmod +x delete_cloudformation.sh
```

Rode o comando **./delete_cloudformation.sh**, assim toda sua infra será removida.
