FROM bitnami/spark:3-debian-10

SHELL [ "/bin/bash", "--login", "-c" ]

USER root

# Debian instalations
RUN install_packages wget

# Create a non-root user
ENV SERVICIO conda-jupyter
ENV USER arquimedes
ENV UID 1000
ENV GID 1000
ENV HOME /home/$USER
RUN adduser --disabled-password \
    --gecos "Non-root user" \
    --uid $UID \
    --home $HOME \
    $USER

# Files for conda installation
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
ENV PROJECT_DIR $HOME/conda_jupyter
RUN mkdir $PROJECT_DIR
WORKDIR $PROJECT_DIR

# build the conda environment
ENV ENV_PREFIX $PROJECT_DIR/system/env
RUN conda update --name base --channel defaults conda && \
    conda env create --prefix $ENV_PREFIX --file /tmp/environment.yml
# run the postBuild script to install any JupyterLab extensions
RUN conda activate $ENV_PREFIX && \
    /usr/local/bin/postBuild.sh && \
    conda deactivate

ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]

# expose port for jupyter lab
EXPOSE 8888

# password for jupyter
ENV JUPYTER_TOKEN eureka

# copy files for utils installation
RUN mkdir -p $PROJECT_DIR/utils/masa_madre/consultas

COPY --chown=$UID:$GID $SERVICIO/setup.py $PROJECT_DIR/utils
COPY --chown=$UID:$GID bases.py $PROJECT_DIR/utils/masa_madre/consultas
RUN touch $PROJECT_DIR/utils/masa_madre/__init__.py $PROJECT_DIR/utils/masa_madre/consultas/__init__.py

# install utils
RUN conda activate $ENV_PREFIX && \
    cd $PROJECT_DIR/utils && \
    python setup.py bdist_wheel && \
    cd dist && \
    pip install masa_madre-1.0-py3-none-any.whl && \
    conda deactivate

# remove utils instalation files
RUN rm $PROJECT_DIR/utils/setup.py && \
    rm -r $PROJECT_DIR/utils/dist $PROJECT_DIR/utils/masa_madre.egg-info $PROJECT_DIR/utils/build

# create empty folder for volume 
RUN mkdir $PROJECT_DIR/src
RUN chown $UID:$GID $PROJECT_DIR/src && \
    chmod u+x $PROJECT_DIR/src

# create empty folder for spark events
RUN mkdir /tmp/spark-events
RUN chown $UID:$GID /tmp/spark-events && \
    chmod u+x /tmp/spark-events

# inicializar el container con jupyter
CMD [ "jupyter", "lab", "--no-browser", "--ip", "0.0.0.0" ]


# comandos útiles:

# image build
# docker image build --file Dockerfile --tag conda-oracle:$IMAGE_TAG ..
