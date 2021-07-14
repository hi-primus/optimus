FROM ubuntu:20.04

RUN apt-get update && yes|apt-get upgrade && \
    apt-get install -y nano && \
    apt-get install -y git && \
    apt-get install -y curl

RUN apt-get install -y wget bzip2

RUN apt-get -y install sudo

RUN apt-get update && apt-get install -y --no-install-recommends apt-utils

RUN wget https://repo.anaconda.com/archive/Anaconda3-2020.07-Linux-x86_64.sh && \
    bash Anaconda3-2020.07-Linux-x86_64.sh -b && \
    rm Anaconda3-2020.07-Linux-x86_64.sh

RUN apt-get install -y net-tools

RUN sudo apt-get update --fix-missing && \
    sudo apt-get install -y gcc && \
    sudo apt-get clean

RUN sudo rm -rf /var/lib/apt/lists/*

RUN sudo apt-get -y update && sudo apt-get -y upgrade && \
    sudo apt-get -y install g++

ENV PATH="/root/anaconda3/bin:${PATH}"

RUN sudo chown -R root ~/anaconda3/bin && \
    sudo chmod -R +x ~/anaconda3/bin

RUN conda install -c conda-forge jupyterlab && \
    conda install -c conda-forge dask-labextension && \
    jupyter serverextension enable dask_labextension && \
    conda install -c conda-forge jupyter_kernel_gateway && \
    conda clean -afy

RUN echo "Version 21.8.0-beta3"

RUN pip install cytoolz --no-cache-dir && \
    pip install git+https://github.com/hi-primus/dateinfer.git --no-cache-dir && \
    pip install git+https://github.com/hi-primus/url_parser.git --no-cache-dir && \
    pip install git+https://github.com/hi-primus/optimus.git@develop-21.8 --no-cache-dir

CMD jupyter notebook --port=8888 --no-browser --ip=0.0.0.0 --allow-root

EXPOSE 8888:8888 8889:8889