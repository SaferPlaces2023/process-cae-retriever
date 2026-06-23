# Copyright (c) 2025 Gecosistema S.r.l.

#FROM ghcr.io/osgeo/gdal:ubuntu-small-3.7.0
FROM 901702069075.dkr.ecr.us-east-1.amazonaws.com/docker-gdal

COPY src /var/tmp/process_cae_retriever/src
COPY pyproject.toml /var/tmp/process_cae_retriever/
WORKDIR /var/tmp/process_cae_retriever 
RUN pip install .
ADD tests /var/task/tests

#Clean up
RUN pip cache purge
RUN apt-get remove -y git && \
    apt-get autoremove -y && \
    apt-get clean
RUN rm -rf /var/tmp/process_cae_retriever/

# AWS Lambda
# copy the entrypoint script to use it like awslinux2
RUN pip install awslambdaric
COPY lambda-entrypoint.sh /lambda-entrypoint.sh
RUN chmod +x /lambda-entrypoint.sh

COPY ./lambda/* /var/task/
WORKDIR /var/task

# Dual-mode processor configuration
# CAE_PROCESSOR_MODE: "local" (default) or "lambda"
# - "local": Run retriever logic locally in the processor (backward compatible)
# - "lambda": Invoke Lambda function for processing (requires CAE_LAMBDA_FUNCTION_NAME)
ENV CAE_PROCESSOR_MODE=lambda
ENV AWS_REGION=us-east-1
# CAE_LAMBDA_FUNCTION_NAME should be set at runtime or via AWS Lambda config

# These following lines are for the AWS Lambda and shoulbe setted on the AWS Lambda function ons aws web console
# or using aws lambda update-function-configuration --function-name <function-name> --handler <handler-name>
# ENTRYPOINT [ "/opt/venv/bin/python", "-m", "awslambdaric" ] for Ubuntu
# ENTRYPOINT [ "/lambda-entrypoint.sh" ] for awslinux2 and Ubuntu
# CMD [ "lambda_safer_rain_function.lambda_handler" ]
CMD ["bash"]
