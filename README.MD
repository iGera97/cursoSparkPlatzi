## CURSO DE FUNDAMENTOS DE SPARK PARA BIG DATA DE PLATZI

##### INSTALACIÓN DE SPARK EN UBUNTU

Esta instalación se realizó en ubuntu 20.04 LTS en el WSL de una máquina Windows, los pasos son los mismos para un ubuntu normal.

       sudo-apt-repository ppa:openjdk-r/ppa
	   sudo add-apt-repository ppa:openjdk-r/ppa
       sudo apt-get -y update
       sudo apt-get -y upgrade
       sudo apt-get -y install openjdk-8-jre
       python3 --version
       sudo apt-get -y install scala
       pip3 --version
       sudo apt install python3-pip
	    sudo pip3 install py4j

Esta versión de ubuntu cuenta con Python 3.8, por lo que se descarga la versión **3.2.1 de spark** del siguiente link: https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz

En la carperta principal /home/usuario creo la carpeta **spark** donde se copia todo el contenido del archivo .tgz que se descargó

    tar -xvf spark-3.2.1-bin-hadoop3.2.tgz
	mv -r spark-3.2.1-bin-hadoop3.2/* /home/usuario/spark/

Se instala anaconda con su versión ejecutable de .sh, la versión usada en este tutorial se obtuvo de: https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh

Posteriormente se ejecuta el archivo Anaconda3-2022.05-Linux-x86_64.sh en la carpeta de usuario:

    sh Anaconda3-2022.05-Linux-x86_64.sh -b
	ls
	->anaconda3 spark
 
 Lo siguiente es modificar nuestro archivo de configuración de la terminal (.bashrc/.zshrc), en este ejemplo se usa una terminal con zsh, por lo que se debe agregar la siguiente linea al archivo .zshrc:

`export PATH="/home/usuario/anaconda3/bin:$PATH"`

Ejecutar `source .zshrc` para que se aplique el cambio.

Y se instala py4j en anaconda:

`conda install py4j `

Finalmente se agregan las siguientes variables al archivo .zshrc

    export SPARK_HOME="/home/gerardo/spark"
	export PATH=$SPARK_HOME:$PATH
	export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
	export PYSPARK_PYTHON=python3
	# PATH DE JAVA
	export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
	export PATH=$JAVA_HOME/bin:$PATH

Ejecutar `source .zshrc` para que se aplique el cambio.

Una vez que tenemos nuestra configuración completa podemos probar ejecutando el archivo codeExample.py

`/home/usuario/spark/bin/spark-submit codeExample.py data.csv `

#### Jupyter vs CLI

Para poder utilizar spark con jupyter_notebook es necesario agregar las siguientes variables al archivo .zshrc:

    export PYSPARK_DRIVER_PYTHON="jupyter"
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

para los que quieren conectar spark con postgresql en ubuntu yo hize lo siguiente baje el driver de JDBC https://jdbc.postgresql.org/download.html
descomprimi el archivo lo copie en descargas y lo pegue en la siguiente ruta /usr/share/java/ quedando
usr/share/java/postgresql-42.3.1.jar, luego exporte la ruta al CLASSPATH con el siguiente comando (export CLASSPATH="$CLASSPATH:/usr/share/java/postgresql-42.3.1.jar"), y quedo listo para usar.
despues en jupyter notebook escribi el siguiente codigo para conectar a la base