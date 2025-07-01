FROM python:3.13.3
WORKDIR /user/local/app

COPY packages ./packages
COPY main.py requirements.txt wait-for-services.sh ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["./wait-for-services.sh"]
CMD [ "python", "main.py" ]