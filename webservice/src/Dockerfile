FROM bigdataengdev.azurecr.io/asos/python3:latest

RUN mkdir /home/recs
# RUN apt-get update 
# RUN apt install -y g++
RUN pip install pydocumentdb rpforest

ADD main /home/recs/main

WORKDIR /home/recs/main

EXPOSE 9090

ENTRYPOINT ["/home/recs/main/init.sh"]
