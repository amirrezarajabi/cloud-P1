from config import *
import pika
import mysql.connector
import requests
import boto3

mydb = mysql.connector.connect(
  host=DATABASE_HOST,
  user=DATABASE_USER,
  password=DATABASE_PASSWORD,
  database=DATABASE_NAME,
  port=DATABASE_PORT,
  auth_plugin='mysql_native_password'
)

s3 = boto3.resource('s3',
        endpoint_url="https://storage.iran.liara.space",
        aws_access_key_id=Access_Key,
        aws_secret_access_key=Secret_Key
)

def send_simple_message(email, msg):
	return requests.post(
		f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
		auth=("api", f"{MAILGUN_API_KEY}"),
		data={"from": f"Excited User <mailgun@{MAILGUN_DOMAIN}>",
			"to": [email],
			"subject": "Result",
			"text": msg})

def get_from_db(username):
  with mydb.cursor() as cursor:
    cursor.execute(
            f"SELECT firstfile,secondfile,email FROM data WHERE username='{username}';"
    ); ls = cursor.fetchall()
    return ls[0]

def get_url_from_s3(ls):
    obj1 = s3.Object(bucket_name=bucket_name, key=ls[0])
    response1 = obj1.get()
    obj2 = s3.Object(bucket_name=bucket_name, key=ls[1])
    response2 = obj2.get()
    return response1['Body'].read(), response2['Body'].read()

def detect_face(face):
    response = requests.post(
    'https://api.imagga.com/v2/faces/detections?return_face_id=1',
    auth=(IMGGA_KEY, IMMGA_SECRET),
    files={'image': face})
    return len(response.json()['result']['faces']) > 0, response.json()

def sim_faces(face_id1, face_id2):
    response = requests.get(
    'https://api.imagga.com/v2/faces/similarity?face_id=%s&second_face_id=%s' % (face_id1, face_id2),
    auth=(IMGGA_KEY, IMMGA_SECRET))
    return response.json()['result']['score']


def set_in_database(username, msg):
    with mydb.cursor() as cursor:
          cursor.execute(
              f"UPDATE data SET state='{msg}' WHERE username='{username}';"
          )
          mydb.commit()

def callback(ch, method, properties, body):
    username = body.decode('utf-8')
    print(username)
    ls = get_from_db(username)
    urls = get_url_from_s3(ls)
    res1 = detect_face(urls[0])
    res2 = detect_face(urls[1])
    if not (res1[0] and res2[0]):
        set_in_database(username, "rejected")
        send_simple_message(ls[-1], "rejected")
    else:
        if sim_faces(res1[1]['result']['faces'][0]['face_id'], res2[1]['result']['faces'][0]['face_id']) > 80:
            set_in_database(username, "accepted")
            send_simple_message(ls[-1], "accepted")
        else:
            set_in_database(username, "rejected")
            send_simple_message(ls[-1], "rejected")





rabbit_params = pika.URLParameters(AMPQ_URL)
rabbit_connection = pika.BlockingConnection(rabbit_params)
channel = rabbit_connection.channel()
channel.queue_declare(queue='jobq')
channel.basic_consume('jobq', callback, auto_ack=True)
channel.start_consuming()

try:
    channel = rabbit_connection.channel()
    channel.queue_declare(queue='jobq')
    channel.basic_consume('jobq',callback,auto_ack=True)
    print(' [*] Waiting for messages:')
    channel.start_consuming()
except Exception:
        print("error")
finally:
    rabbit_connection.close()
