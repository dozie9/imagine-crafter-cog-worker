import base64
import mimetypes
import time
import os
import json
import subprocess
import uuid

import runpod
import requests
import ffmpeg
from firebase_admin import credentials, initialize_app, storage, firestore

from requests.adapters import HTTPAdapter, Retry
from runpod.serverless.utils.rp_validator import validate
from runpod.serverless.modules.rp_logger import RunPodLogger


logger = RunPodLogger()

SERVICE_CERT = json.loads(os.environ["FIREBASE_KEY"])
SADTALKER_SERVICE_CERT = json.loads(os.environ["SADTALKER_FIREBASE_KEY"])
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]

cred_obj = credentials.Certificate(SERVICE_CERT)
sad_cred_obj = credentials.Certificate(SADTALKER_SERVICE_CERT)

default_app = initialize_app(cred_obj, {"storageBucket": STORAGE_BUCKET}, name="ImagineCrafter")
sad_app = initialize_app(sad_cred_obj, name='sadtalker')


LOCAL_URL = "http://127.0.0.1:5000"

cog_session = requests.Session()
retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[502, 503, 504])
cog_session.mount('http://', HTTPAdapter(max_retries=retries))


INPUT_SCHEMA = {
    "leonard_payload": {
        "type": dict,
        "required": True
    },
    "dynami_payload": {
        "type": dict,
        "required": True
    },
    "user_id": {
        "type": str,
        "required": True
    }
}

DYNAMI_INPUT_SCHEMA = {
    # 'i2v_input_image': {
    #     'type': str,
    #     'required': True
    # },
    'i2v_input_text': {
        'type': str,
        'required': False,
        'default': 'man fishing in a boat at sunset',
    },
    'i2v_seed': {
        'type': int,
        'required': False,
        'default': 123,
        'constraints': lambda i2v_seed: i2v_seed <= 10000
    },
    'i2v_eta': {
        'type': float,
        'required': False,
        'default': 1.0,
        'constraints': lambda i2v_eta: i2v_eta <= 1
    },
    'i2v_cfg_scale': {
        'type': float,
        'required': False,
        'default': 7.5,
        'constraints': lambda i2v_cfg_scale: 1 <= i2v_cfg_scale <= 15
    },
    'i2v_steps': {
        'type': int,
        'required': False,
        'default': 50,
        'constraints': lambda i2v_steps: 1 <= i2v_steps <= 60
    },
    'i2v_motion': {
        'type': int,
        'required': False,
        'default': 4,
        'constraints': lambda i2v_motion: 1 <= i2v_motion <= 20
    },
}


la_headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": f"Bearer {os.environ['LEONARD_API_KEY']}"
}


# ----------------------------- Start API Service ---------------------------- #
# Call "python -m cog.server.http" in a subprocess to start the API service.
subprocess.Popen(["python", "-m", "cog.server.http"])


# ---------------------------------------------------------------------------- #
#                              Automatic Functions                             #
# ---------------------------------------------------------------------------- #
def wait_for_service(url):
    '''
    Check if the service is ready to receive requests.
    '''
    while True:
        try:
            health = requests.get(url, timeout=120)
            status = health.json()["status"]

            if status == "READY":
                time.sleep(1)
                return

        except requests.exceptions.RequestException:
            print("Service not ready yet. Retrying...")
        except Exception as err:
            print("Error: ", err)

        time.sleep(0.2)


def run_inference(inference_request):
    '''
    Run inference on a request.
    '''
    response = cog_session.post(url=f'{LOCAL_URL}/predictions',
                                json=inference_request, timeout=600)
    return response.json()


def send_leonard_request(data):
    url = "https://cloud.leonardo.ai/api/rest/v1/generations"
    data = data['leonard_payload']
    r = requests.post(url, json=data, headers=la_headers)

    return r.json()


def get_leonard_generation(generation_id):
    url = f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}"
    r = requests.get(url, headers=la_headers)
    return r.json()


def get_image(data):
    logger.info('Receiving image from midjourney...')

    res = send_leonard_request(data)
    # print(res)
    try:
        generation_id = res['sdGenerationJob']['generationId']
    except KeyError:
        return res
    generation_data = get_leonard_generation(generation_id)
    # print(generation_data)
    status = generation_data['generations_by_pk']['status']

    generated_images = generation_data['generations_by_pk']['generated_images']

    while status not in ['COMPLETE', 'FAILED']:
        generation_data = get_leonard_generation(generation_id)
        status = generation_data['generations_by_pk']['status']

        time.sleep(5)
    #
    logger.info('Image received...')
    return generation_data


def get_extension_from_mime(mime_type):
    extension = mimetypes.guess_extension(mime_type)
    return extension


def to_file(data: str):
    # bs4_code = data.split(';base64,')[-1]

    # Splitting the input string to get the MIME type and the base64 data
    split_data = data.split(",")
    mime_type = split_data[0].split(":")[1].split(';')[0]
    base64_data = split_data[1]

    ext = get_extension_from_mime(mime_type)
    f_name = f'{uuid.uuid4()}'
    filename = f'{f_name}{ext}'
    decoded_data = base64.b64decode(base64_data)

    with open(filename, 'wb') as f:
        f.write(decoded_data)

    generate_thumbnail(f_name, ext)
    video_url = upload_file(filename)
    thumbnail_url = upload_file(f'{f_name}.png', folder='Dynamic Crafter + Midjourney/thumbnail')

    return video_url, thumbnail_url


def to_firestore(video_url, thumbnail_url, prompt, user_id):
    db = firestore.client(app=sad_app)
    push_data = {
        "addToFeed": False,
        "commentsCount": 0,
        "likes": [],
        "shares": [],
        "thumbnail": thumbnail_url,
        "uploaderId": user_id,
        "videoCaption": prompt,
        "videoUrl": video_url,
    }

    collection_path = "videosList"

    print("*************Starting firestore data push***************")
    update_time, firestore_push_id = db.collection(collection_path).add(
        push_data
    )

    print(update_time, firestore_push_id)


def generate_thumbnail(f_name: str, extension: str):
    (
        ffmpeg.input(f'{f_name}{extension}', ss="00:00:1")
        .output(f"{f_name}.png", vframes=1)
        .run()
    )


def upload_file(filename, folder='Dynamic Crafter + Midjourney'):
    destination_blob_name = f'{folder}/{filename}'
    bucket = storage.bucket(app=default_app)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(filename)

    # Opt : if you want to make public access from the URL
    blob.make_public()

    logger.info("File uploaded to firebase...")
    return blob.public_url
# ---------------------------------------------------------------------------- #
#                                RunPod Handler                                #
# ---------------------------------------------------------------------------- #
def handler(event):
    '''
    This is the handler function that will be called by the serverless.
    '''

    validated_input = validate(event['input'], INPUT_SCHEMA)

    if 'errors' in validated_input:
        logger.error('Error in input...')
        return {
            'errors': validated_input['errors']
        }

    # validate dynami-crafter input
    dynami_payload = validated_input['validated_input']['dynami_payload']
    validated_dynami_payload = validate(dynami_payload, DYNAMI_INPUT_SCHEMA)

    if 'errors' in validated_dynami_payload:
        logger.error('Error in input dynami payload...')
        return {
            'errors': validated_dynami_payload['errors']
        }

    logger.info('Input validated...')

    valid_input = validated_input['validated_input']
    leonard_data = get_image(valid_input)

    try:
        valid_input['dynami_payload'].update(
            {'i2v_input_image': leonard_data['generations_by_pk']['generated_images'][0]['url']}
        )
    except KeyError:
        return leonard_data

    result = run_inference({"input": valid_input['dynami_payload']})

    # convert crafter data to file and upload to firebase
    urls = to_file(result['output'])

    # write info to firestore
    to_firestore(
        video_url=urls[0], thumbnail_url=urls[1],
        prompt=valid_input['leonard_payload']['prompt'],
        user_id=valid_input['user_id']
    )

    return {
        'video_url': urls[0],
        'thumbnail': urls[1],
        'prompt': valid_input['leonard_payload']['prompt'],
        'user_id': valid_input['user_id']
    }


if __name__ == "__main__":
    wait_for_service(url=f'{LOCAL_URL}/health-check')

    print("Cog API Service is ready. Starting RunPod serverless handler...")

    runpod.serverless.start({"handler": handler})
