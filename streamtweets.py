import tweepy
import socket
import json
import preprocessor as p
import pandas as pd
import sys
import requests
import requests_oauthlib



consumer_key = "A62LwQY80obt6YDx6XMYuXBG2"
consumer_secret = "qbEKoqBLSJEV8QnSZsCkHz6M7lL1NozDcbjZGfGCMuPyqJHqHu"
access_key = "1518937046370316288-eL6EfRuOhyNN7RjWdEXXqytNdmk6tF"
access_secret = "sbVfzq2QglWQA25Cnv1FtPPV0dhgSJ7wuI0P0ZEE287dm"
conn = None
class IDPrinter(tweepy.Stream):
    def sock(self, csocket):
        self.client_socket = csocket

    def on_data(self, raw_data):
        # print(status.id)
        # print(status.text)
        # print(status.created_at)
        # print(type(status))
        #print(raw_data)
        try:
            data = json.loads(raw_data)
            #print("helooo")
            if "extended_tweet" in data:

                clean_text = data["extended_tweet"]["full_text"]
            else:
                clean_text = data["text"]
            print(str(clean_text))
            conn.send((clean_text + "\n").encode())
            return True
        except BaseException as e:
            print("Error: " + str(e))


if __name__ == '__main__':



    TCP_IP = "localhost"
    TCP_PORT = 9009
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(300)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    

    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    print("Connected.Starting getting tweets.")
    print(conn)
    printer = IDPrinter(consumer_key, consumer_secret,access_key, access_secret)
    printer.sock(conn)
    printer.filter(track=['#bitcoin','#tesla','#school','#elon','#NFT','#india','#honest','#modi','#metaverse'])
    

    s.close()

    