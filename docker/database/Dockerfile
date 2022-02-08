FROM ubuntu

SHELL [ "/bin/bash", "--login", "-c" ]

# Create a non-root user
ENV SERVICIO database
ENV USER arquimedes
ENV UID 1000
ENV GID 1000
ENV HOME /home/$USER
RUN adduser --disabled-password \
    --gecos "Non-root user" \
    --uid $UID \
    --home $HOME \
    $USER

RUN apt-get update
RUN apt-get install wget -y

COPY $SERVICIO/environment.yml requirements.txt /tmp/
RUN chown $UID:$GID /tmp/environment.yml /tmp/requirements.txt

COPY postBuild /usr/local/bin/postBuild.sh
RUN chown $UID:$GID /usr/local/bin/postBuild.sh && \
    chmod u+x /usr/local/bin/postBuild.sh

COPY $SERVICIO/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chown $UID:$GID /usr/local/bin/entrypoint.sh && \
    chmod u+x /usr/local/bin/entrypoint.sh

USER $USER
# install miniconda
ENV MINICONDA_VERSION latest
ENV CONDA_DIR $HOME/miniconda3
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-$MINICONDA_VERSION-Linux-x86_64.sh -O ~/miniconda.sh && \
    chmod +x ~/miniconda.sh && \
    ~/miniconda.sh -b -p $CONDA_DIR && \
    rm ~/miniconda.sh
# make non-activate conda commands available
ENV PATH=$CONDA_DIR/bin:$PATH
# make conda activate command available from /bin/bash --login shells
RUN echo ". $CONDA_DIR/etc/profile.d/conda.sh" >> ~/.profile
# make conda activate command available from /bin/bash --interative shells
RUN conda init bash

# create a project directory inside user home
ENV PROJECT_DIR $HOME/load_bases
RUN mkdir $PROJECT_DIR
WORKDIR $PROJECT_DIR

# build the conda environment
ENV ENV_PREFIX $PROJECT_DIR/env
RUN conda update --name base --channel defaults conda && \
    conda env create --prefix $ENV_PREFIX --file /tmp/environment.yml
# run the postBuild script to install any JupyterLab extensions
RUN conda activate $ENV_PREFIX && \
    /usr/local/bin/postBuild.sh && \
    conda deactivate

ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]

# copy source and data files 
COPY --chown=$UID:$GID $SERVICIO/data/proveedores $PROJECT_DIR/data
COPY --chown=$UID:$GID $SERVICIO/data/hadoop $PROJECT_DIR/hadoop_data
COPY --chown=$UID:$GID bases.py $PROJECT_DIR/bases.py
COPY --chown=$UID:$GID $SERVICIO/load_bases.py $PROJECT_DIR/load_bases.py

# unzip databases
RUN tar xvzf $PROJECT_DIR/data/dw_bases_pesadas.tar.gz -C $PROJECT_DIR/data && \
    rm $PROJECT_DIR/data/dw_bases_pesadas.tar.gz

RUN tar xvzf $PROJECT_DIR/data/dw_bases_livianas.tar.gz -C $PROJECT_DIR/data && \
    rm $PROJECT_DIR/data/dw_bases_livianas.tar.gz

RUN tar xvzf $PROJECT_DIR/data/dwcla_bases_relevantes.tar.gz -C $PROJECT_DIR/data && \
    rm $PROJECT_DIR/data/dwcla_bases_relevantes.tar.gz

RUN tar xvzf $PROJECT_DIR/hadoop_data/data_hadoop.tar.gz -C $PROJECT_DIR/hadoop_data && \
    rm $PROJECT_DIR/hadoop_data/data_hadoop.tar.gz

# inicializar las bases
CMD [ "python", "load_bases.py"]


# comandos útiles:

# image build
# docker image build --file Dockerfile --tag conda-oracle:$IMAGE_TAG ..
