FROM public.ecr.aws/lambda/python:3.9

ARG ENV

WORKDIR ${LAMBDA_TASK_ROOT}

RUN curl https://install.python-poetry.org | python3 -

COPY . .

RUN $HOME/.local/bin/poetry config virtualenvs.create false
RUN $HOME/.local/bin/poetry install

CMD ["lambda.handler"]