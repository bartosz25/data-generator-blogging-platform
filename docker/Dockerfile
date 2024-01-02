FROM python:3.8

RUN apt-get update -qq && \
  apt-get install -y python3-pip && \
  apt-get install -y librdkafka-dev && \
  apt-get clean

RUN pip install --upgrade pip  && \
  pip3 install virtualenv


RUN useradd -ms /bin/bash data_generator_user
USER data_generator_user
WORKDIR /home/data_generator_user
RUN chown -R data_generator_user:data_generator_user /home/data_generator_user

COPY requirements.txt ./
COPY data_generator ./data_generator

RUN virtualenv -p python3 .venv/
RUN /bin/bash -c "source .venv/bin/activate"

RUN pip3 install -r requirements.txt

# To avoid import problems; can be also solved at the code level with sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
ENV PYTHONPATH="${PYTHONPATH}:."

RUN mkdir -p /home/data_generator_user/data_generator_output
VOLUME /home/data_generator_user/data_generator_output

ENTRYPOINT python3 /home/data_generator_user/data_generator/generate_dataset.py --config_file /tmp/not_existing.yaml

