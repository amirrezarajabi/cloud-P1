from flask import Flask, request, jsonify
import hashlib
import mysql.connector
import boto3
from config import *
import pika
from os import path as osp
import time

s3 = boto3.resource('s3',
        endpoint_url="https://storage.iran.liara.space",
        aws_access_key_id=Access_Key,
        aws_secret_access_key=Secret_Key
)

mydb = mysql.connector.connect(
  host=DATABASE_HOST,
  user=DATABASE_USER,
  password=DATABASE_PASSWORD,
  database=DATABASE_NAME,
  port=DATABASE_PORT,
  auth_plugin='mysql_native_password'
)

rabbit_params = pika.URLParameters(AMPQ_URL)
rabbit_connection = pika.BlockingConnection(rabbit_params)


def publish_on_rabbit(content):
  global rabbit_connection
  while rabbit_connection.is_closed:
    rabbit_connection = pika.BlockingConnection(rabbit_params)
    time.sleep(0.1)
  channel = rabbit_connection.channel()
  channel.queue_declare(queue='jobq')
  channel.basic_publish(exchange='',routing_key='jobq',body=content)

app = Flask(__name__)

@app.route('/register', methods=['POST'])
def register():
    try:
      email = request.form['email']
      name = request.form['name']
      national_id = request.form['national_id']
      username = hashlib.md5(str(name + email + national_id).encode()).hexdigest()
      res_db = check_db(national_id)
      if res_db[0]:
         return "you already have a request"
      national_id = hashlib.md5(str(national_id).encode()).hexdigest()
      ip_address = request.remote_addr
      photo1 = request.files['photo1']
      photo2 = request.files['photo2']
      f1, extension = osp.splitext(osp.basename(photo1.filename))
      object = s3.Object(bucket_name, f"{username}_0{extension}")
      _ = object.put(Body=photo1.read())
      f2, extension = osp.splitext(osp.basename(photo2.filename))
      object = s3.Object(bucket_name, f"{username}_1{extension}")
      _ = object.put(Body=photo2.read())
      firstfile = f"{username}_0{extension}"
      secondfile = f"{username}_1{extension}"

      if res_db[0] == None:
        with mydb.cursor() as cursor:
          cursor.execute(
              f"INSERT INTO data (ip_add, username, email, name, nationalID, firstfile, secondfile, state) VALUES ('{ip_address}', '{username}', '{email}', '{name}', '{national_id}', '{firstfile}', '{secondfile}', 'ongoing');"
          )
          mydb.commit()
      else:
         with mydb.cursor() as cursor:
          cursor.execute(
              f"UPDATE data SET username='{username}', email='{email}', name='{name}', nationalID'{national_id}', firstfile='{firstfile}', secondfile='{secondfile}', state='ongoing', ip_add='{ip_address}') WHERE nationalID='{national_id}';"
          )
          mydb.commit()
      print('added to dataset')
      publish_on_rabbit(username)
      print("DONE")
      return "your request is submitted."
    except Exception as e:
        return e

def check_db(n_id):
  nid = hashlib.md5(str(n_id).encode()).hexdigest()
  print(nid)
  with mydb.cursor() as cursor:
    cursor.execute(
            f"SELECT state,ip_add,username FROM data WHERE nationalID='{nid}';"
    ); ls = cursor.fetchall()
    print(ls)
    if len(ls) == 0:
       return None, None
    if ls[0][0] == "ongoing":
       return True, ls[0]
    else:
       return False, ls[0]

@app.route('/status', methods=['GET'])
def status():
    ip_add = request.remote_addr
    n_id = request.args.get('national_id')
    res = check_db(n_id)
    if res[1][1] != ip_add:
       return "your ip add is changed, No access for unauthorized users"
    if res[0] == None:
       return "There is no request"
    if res[1][0] == "accepted":
       return f"accepted, you are {res[1][2]}"
    return res[1][0]
app.run(debug=True, port=11011)