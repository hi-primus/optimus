# Run jdbc test


## Run containers
First you need to run the containers
mysql://SA:SA123@localhost/optimus


### Mysql
```
sudo docker run --name=mysql -d -p 3306:3306 -e MYSQL_USER=SA -e MYSQL_PASSWORD=SA123 -e MYSQL_DATABASE=optimus mysql/mysql-server
```

if you already run the last command just start the container

```
sudo docker start mysql
```

### Postgres
```
sudo docker run --name postgres --e POSTGRES_USER=SA -e POSTGRES_PASSWORD=SA123 -e POSTGRES_DB=optimus -p 5432:5432 -d postgres
```

### Redshit
This is just postgres with some confgiruation options to looks like redshit
```
sudo docker run --name=redshift -d -p 5439:5439 -e POSTGRES_USER=SA -e POSTGRES_PASSWORD=SA123 -e POSTGRES_DB=optimus guildeducation/docker-amazon-redshift
```

### Mssql server
```
sudo docker run --name=mssql -d -p 1433:1433  -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=SA123' microsoft/mssql-server-linux:2017-latest
```

## Helpers

Connect to container
```
sudo docker exec -i -t container_name /bin/bash
```

List containers running
```
sudo docker ps -a
```

List images
```
sudo docker image ls
```

List containers
```
sudo dokcer container ls
```

Remove container
First you need to stop the container so you can remove it

```
sudo docker stop container_name
sudo docker rm container_name
```
 