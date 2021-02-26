FROM ntm2004/myaws

COPY ./createS3.py /app/createS3.py

WORKDIR /app

RUN pip install boto3

ENTRYPOINT [ "python" ]

CMD [ "createS3.py" ]