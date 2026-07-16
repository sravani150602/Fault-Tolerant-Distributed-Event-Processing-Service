FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install -r ${LAMBDA_TASK_ROOT}/requirements.txt --no-cache-dir

COPY src/ ${LAMBDA_TASK_ROOT}/src/

CMD ["src.handlers.lambda_handler.lambda_handler"]
